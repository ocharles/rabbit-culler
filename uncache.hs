{-# LANGUAGE OverloadedStrings #-}
import Prelude hiding (lookup)
import Control.Applicative ((<$>), (<*>), pure)
import Control.Monad (void)
import Data.Traversable (traverse, Traversable)
import Data.Aeson (FromJSON(..), decode, (.:), (.:?), Object, Value(Object))
import Data.Aeson.Types (Parser)
import Data.List ()
import qualified Data.Set as Set
import Data.Time.Clock (diffUTCTime, getCurrentTime, UTCTime)
import Data.Time.Format
import System.Locale (defaultTimeLocale)
import Data.Maybe (catMaybes, fromJust)
import Data.Map ()
import Network.AMQP ( Ack(..), Message(..), Envelope, bindQueue, closeConnection
                    , consumeMsgs, declareExchange, declareQueue, exchangeName
                    , exchangeType, newExchange, newQueue, openConnection
                    , openChannel, queueExclusive)
import qualified Network.Memcache as MC
import qualified Network.Memcache.Protocol as MCS

{-| Represents an event in the message queue.

Events have the table that fired the event, along with 'old' and 'new' values,
which map to the OLD and NEW values inside the trigger.
-}
data Event = Event { oldEvent :: Maybe Row, newEvent :: Maybe Row
                   , eventTimestamp :: UTCTime }
  deriving (Show)

{-| Represents a row in the database that has changed. -}
data Row = ArtistType Int
         | ArtistCredit Int
         | Artist Int
         | ArtistMBID String
         | Country Int
         | Gender Int
         | Label Int
         | LabelMBID String
         | Language Int
         | LabelType Int
         | Link Int
         | LinkType Int
         | LinkAttributeType Int
         | MediumFormat Int
         | ReleaseGroupType Int
         | ReleaseStatus Int
         | ReleasePackaging Int
         | Script Int
         | WorkType Int
  deriving (Show)

jsonToRow :: String -> Object -> Parser Row
jsonToRow tableName o = case tableName of
  "artist_type"         -> ArtistType <$> o .: "id"
  "artist_credit"       -> ArtistCredit <$> o .: "id"
  "artist_credit_name"  -> ArtistCredit <$> o .: "artist_credit"
  "artist"              -> Artist <$> o .: "id"
  "artist_gid_redirect" -> ArtistMBID <$> o .: "gid"
  "country"             -> Country <$> o .: "id"
  "gender"              -> Gender <$> o .: "id"
  "label"               -> Label <$> o .: "id"
  "label_gid_redirect"  -> LabelMBID <$> o .: "gid"
  "language"            -> Language <$> o .: "id"
  "label_type"          -> LabelType <$> o .: "id"
  "link"                -> Link <$> o .: "id"
  "link_type"           -> LinkType <$> o .: "id"
  "link_attribute_type" -> LinkAttributeType <$> o .: "id"
  "medium_format"       -> MediumFormat <$> o .: "id"
  "release_group_type"  -> ReleaseGroupType <$> o .: "id"
  "release_status"      -> ReleaseStatus <$> o .: "id"
  "release_packaging"   -> ReleasePackaging <$> o .: "id"
  "script"              -> Script <$> o .: "id"
  "work_type"           -> WorkType <$> o .: "id"
  _                     -> error $ "Unknown table " ++ tableName

{- This is where most of the magic happens!

This maps a event from the message queue into a suitable value for the uncache
daemon. It first inspects the 'tableName' value of the event, and then
dispatches the rest of deserialization based on this value. The end result
is we have a well typed Event value, with either (or both) new or old records.
-}
instance FromJSON (Event) where
  parseJSON json = case json of
    (Object v) -> do
      tableName <- v .: "table"
      timestamp <- v .: "timestamp" :: Parser String
      let mapper = jsonToRow tableName
      Event <$> (v .:? "old" >>= traverse mapper)
            <*> (v .:? "new" >>= traverse mapper)
            <*> pure (readTime defaultTimeLocale "%s%Q" timestamp :: UTCTime)
    _ -> fail "Expected JSON object"

{-| The underlying event handling. Takes the message body (ByteString), and
transforms it to an 'Event' value. It then uncaches the old/new values of this
event, depending on their presence.
-}
handleEvent :: (Message, Envelope) -> IO ()
handleEvent (message, _) = do
  let event = fromJust $ decode $ msgBody message :: Event
  mapM_ uncache $
    Set.elems $ Set.fromList $
      concat $
        catMaybes [ fmap determineKeys (oldEvent event)
                  , fmap determineKeys (newEvent event)
                  ]
  now <- getCurrentTime
  putStrLn $ "Lag: " ++ show (diffUTCTime now $ eventTimestamp event)

{-| This is what maps a row to a cache key.

Given a row, attempt to extract a list of cache keys from it. -}
determineKeys :: Row -> [String]
determineKeys r = case r of
  (ArtistType id') -> [ "at:" ++ show id', "at:all" ]
  (ArtistCredit id') -> [ "ac:" ++ show id' ]
  (Artist id') -> [ "artist:" ++ show id' ]
  (ArtistMBID mbid) -> [ "artist:" ++ mbid ]
  (Country id') -> [ "c:" ++ show id', "c:all" ]
  (Gender id') -> [ "g:" ++ show id', "g:all" ]
  (Label id') -> [ "label:" ++ show id' ]
  (LabelMBID mbid) -> [ "label:" ++ mbid ]
  (LabelType id') -> [ "lt:" ++ show id', "lt:all" ]
  (Language id') -> [ "lng:" ++ show id', "lng:all" ]
  (Link id') -> [ "link:" ++ show id' ]
  (LinkType id') -> [ "linktype:" ++ show id' ]
  (LinkAttributeType id') -> [ "linkattrtype:" ++ show id' ]
  (MediumFormat id') -> [ "mf:" ++ show id', "mf:all" ]
  (ReleaseGroupType id') -> [ "rgt:" ++ show id', "rgt:all" ]
  (ReleaseStatus id') -> [ "rs:" ++ show id', "rs:all" ]
  (ReleasePackaging id') -> [ "rp:" ++ show id', "rp:all" ]
  (Script id') -> [ "scr:" ++ show id', "scr:all" ]
  (WorkType id') -> [ "wt:" ++ show id', "wt:all" ]

{-| Backend logic of actually getting to memcached and doing the uncaching. -}
uncache :: String -> IO ()
uncache key = do
  memcached <- MCS.connect "localhost" 11211
  putStrLn $ "Uncaching " ++ key
  void $ MC.delete memcached key 0

{-| Run it! -}
main :: IO ()
main = do
  putStr "Initializing... "
  rabbitConn <- openConnection "127.0.0.1" "/" "guest" "guest"
  rabbitChan <- openChannel rabbitConn
  declareExchange rabbitChan newExchange { exchangeName = "musicbrainz"
                                         , exchangeType = "fanout"
                                         }
  declareQueue rabbitChan newQueue { queueExclusive = True }
  bindQueue rabbitChan "" "musicbrainz" ""

  consumeMsgs rabbitChan "" NoAck handleEvent

  putStrLn "Done!"

  -- Keep this daemon open until the user presses a key
  -- (consumeMsgs forks)
  getLine >> closeConnection rabbitConn

  return ()
