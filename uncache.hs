{-# LANGUAGE OverloadedStrings #-}
import Prelude hiding (lookup)
import Control.Applicative ((<$>), (<*>))
import Control.Monad (void)
import Data.Traversable (traverse, Traversable)
import Data.Aeson (FromJSON(..), decode, (.:), (.:?), Value(Object))
import Data.Aeson.Types (Parser)
import Data.List ()
import qualified Data.Set as Set
import Data.Text (Text)
import Data.Maybe (catMaybes, fromJust)
import Data.Map ()
import Network.AMQP ( Ack(..), Message(..), Envelope, bindQueue, closeConnection
                    , consumeMsgs, declareExchange, declareQueue
                    , exchangeDurable, exchangeName, exchangeType, newExchange
                    , newQueue, openConnection, openChannel, queueExclusive)
import qualified Network.Memcache as MC
import qualified Network.Memcache.Protocol as MCS

{-| Represents an event in the message queue.

Events have the table that fired the event, along with 'old' and 'new' values,
which map to the OLD and NEW values inside the trigger.
-}
data Event = Event { oldEvent :: Maybe Row, newEvent :: Maybe Row }
  deriving (Show)

{-| Represents a row in the database that has changed. -}
data Row = ArtistType { artistTypeId :: Int }
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
    let mapper =
          case tableName of
            "artist_type" -> (\o -> ArtistType <$> o .: "id")
            _ -> error $ "Unknown table " ++ show tableName
    Event <$> (v .:? "old" >>= traverse mapper)
          <*> (v .:? "new" >>= traverse mapper)

{-| The underlying event handling. Takes the message body (ByteString), and
transforms it to an 'Event' value. It then uncaches the old/new values of this
event, depending on their presence.
-}
handleEvent :: (Message, Envelope) -> IO ()
handleEvent (message, _) = do
  let event = fromJust $ decode $ msgBody message :: Event
  traverse uncache (oldEvent event)
  traverse uncache (newEvent event)
  return ()

{-| This is what maps a row to a cache key.

Given a row, attempt to extract the cache key from it. -}
uncache :: Row -> IO ()
uncache (ArtistType id) = uncache' $ "at:" ++ show id

{-| Backend logic of actually getting to memcached and doing the uncaching. -}
uncache' :: String -> IO ()
uncache' key = do
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
