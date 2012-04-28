{-# LANGUAGE OverloadedStrings #-}
import Prelude hiding (lookup)
import Control.Applicative ((<$>), (<*>), pure)
import Control.Monad (void)
import Data.Traversable (traverse, Traversable)
import Data.Aeson (FromJSON(..), decode, (.:), (.:?), Value(Object))
import Data.Aeson.Types (Parser)
import Data.List ()
import qualified Data.Set as Set
import Data.Text (Text)
import Data.Time.Clock (diffUTCTime, getCurrentTime, UTCTime)
import Data.Time.Format
import System.Locale (defaultTimeLocale)
import Data.Maybe (catMaybes, fromJust)
import Data.Map ()
import Network.AMQP ( Ack(..), Message(..), Envelope, bindQueue, closeConnection
                    , consumeMsgs, declareExchange, declareQueue
                    , exchangeDurable, exchangeName, exchangeType, msgTimestamp
                    , newExchange, newQueue, openConnection, openChannel
                    , queueExclusive)
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
data Row = ArtistType { artistTypeId :: Int }
         | ArtistCredit { artistCreditId :: Int }
         | Artist { artistId :: Int }
         | ArtistMBID { artistMBID :: String }
  deriving (Show)

{- This is where most of the magic happens!

This maps a event from the message queue into a suitable value for the uncache
daemon. It first inspects the 'tableName' value of the event, and then
dispatches the rest of deserialization based on this value. The end result
is we have a well typed Event value, with either (or both) new or old records.
-}
instance FromJSON (Event) where
  parseJSON (Object v) = do
    tableName <- v .: "table" :: Parser Text
    timestamp <- v .: "timestamp" :: Parser String
    let mapper =
          case tableName of
            "artist_type" -> (\o -> ArtistType <$> o .: "id")
            "artist_credit" -> (\o -> ArtistCredit <$> o .: "id")
            "artist_credit_name" -> (\o -> ArtistCredit <$> o .: "artist_credit")
            "artist" -> (\o -> Artist <$> o .: "id")
            "artist_gid_redirect" -> (\o -> ArtistMBID <$> o .: "gid")
            _ -> error $ "Unknown table " ++ show tableName
    Event <$> (v .:? "old" >>= traverse mapper)
          <*> (v .:? "new" >>= traverse mapper)
          <*> pure (readTime defaultTimeLocale "%s%Q" timestamp :: UTCTime)

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
  print $ "Lag: " ++ show (diffUTCTime now $ eventTimestamp event)

{-| This is what maps a row to a cache key.

Given a row, attempt to extract a list of cache keys from it. -}
determineKeys :: Row -> [String]
determineKeys (ArtistType id) = [ "at:" ++ show id
                                , "at:all"
                                ]
determineKeys (ArtistCredit id) = [ "ac:" ++ show id ]
determineKeys (Artist id) = [ "artist:" ++ show id ]
determineKeys (ArtistMBID mbid) = [ "artist:" ++ mbid ]

{-| Backend logic of actually getting to memcached and doing the uncaching. -}
uncache :: String -> IO ()
uncache key = do
  memcached <- MCS.connect "localhost" 11211
  putStrLn $ "Uncaching " ++ key
  void $ MC.delete memcached key 0

{-| Run it! -}
main :: IO ()
main = do
  rabbitConn <- openConnection "127.0.0.1" "/" "guest" "guest"
  rabbitChan <- openChannel rabbitConn
  declareExchange rabbitChan newExchange { exchangeName = "musicbrainz"
                                         , exchangeType = "fanout"
                                         }
  declareQueue rabbitChan newQueue { queueExclusive = True }
  bindQueue rabbitChan "" "musicbrainz" ""

  consumeMsgs rabbitChan "" NoAck handleEvent

  -- Keep this daemon open until the user presses a key
  -- (consumeMsgs forks)
  getLine >> closeConnection rabbitConn

  return ()
