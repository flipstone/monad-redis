module Control.Monad.Redis where

import            Control.Applicative
import            Control.Monad
import            Control.Monad.Base
import            Control.Monad.Catch
import            Control.Monad.Except
import            Control.Monad.IO.Class
import            Control.Monad.Reader
import            Control.Monad.Trans
import            Control.Monad.Trans.Control
import            Control.Monad.Trace
import qualified  Data.ByteString.Char8 as BS
import            Data.Monoid
import            Data.Time
import            Database.Redis hiding (MonadRedis, liftRedis)
import            System.IO

class MonadIO m => MonadRedis m where
  redisConn :: m Connection

  liftRedis :: Redis a -> m a
  liftRedis action = do
    redis <- redisConn
    liftIO $ runRedis redis action

newtype RedisT m a = RedisT { unRedisT :: ReaderT Connection m a }
  deriving ( Functor
           , Applicative
           , Alternative
           , Monad
           , MonadPlus
           , MonadIO
           , MonadThrow
           , MonadCatch
           , MonadMask
           )

deriving instance MonadBase b m => MonadBase b (RedisT m)
deriving instance MonadError e m => MonadError e (RedisT m)

instance MonadBaseControl b m => MonadBaseControl b (RedisT m) where
  type StM (RedisT m) a = StM m a

  liftBaseWith f = RedisT $ liftBaseWith $ \runInBase ->
                    f (\(RedisT m) -> runInBase m)

  restoreM stm = RedisT (restoreM stm)

instance MonadTrans RedisT where
  lift = RedisT . lift

instance MonadIO m => MonadRedis (RedisT m) where
  redisConn = RedisT ask

instance MonadIO m => MonadTraceSink (RedisT m) where
  traceSink = redisTraceSink

runRedisT :: RedisT m a -> Connection -> m a
runRedisT = runReaderT . unRedisT

mapRedisT :: (m a -> n b) -> RedisT m a -> RedisT n b
mapRedisT f = RedisT . mapReaderT f . unRedisT

redisTraceSink :: (MonadIO m, MonadRedis m) => TraceSink m
redisTraceSink traceKey chunks = do
  result <- liftRedis $ append traceKey (BS.concat chunks)

  case result of
    Right _ -> pure ()
    Left err -> liftIO $ do
      BS.hPutStrLn stderr $ BS.concat
         [ "Error while logging to redis key "
         , traceKey
         , ":\n    "
         , BS.pack (show err)
         ]

newRedisTraceKey :: (MonadIO m, MonadRedis m) => TraceKey -> Integer -> m TraceKey
newRedisTraceKey prefix expirySeconds = do
  key <- newTraceKey "trace:" (":" <> prefix)
  void $ liftRedis $ setex key expirySeconds ""
  pure $ key

withRedisTraceKey :: (MonadRedis m, MonadIO m, MonadTrace m)
                  => TraceKey -> Integer -> m a -> m (TraceKey, m a)
withRedisTraceKey prefix expirySeconds ma = do
  key <- newRedisTraceKey prefix expirySeconds
  traceLogs [ "\n<<< OTHER LOG KEY ", key ," >>>\n"]
  pure (key, withTraceKey (const key) ma)

withRedisTrace :: (MonadRedis m, MonadIO m, MonadTrace m)
               => TraceKey -> Integer -> m a -> m a
withRedisTrace prefix expirySeconds = join . fmap snd . withRedisTraceKey prefix expirySeconds

newtype RedisTraceT m a = RedisTraceT { unRedisTraceT :: TraceT (RedisT m) a }
  deriving ( Functor
           , Applicative
           , Alternative
           , Monad
           , MonadPlus
           , MonadIO
           , MonadThrow
           , MonadCatch
           , MonadMask
           )

deriving instance MonadBase b m => MonadBase b (RedisTraceT m)
deriving instance MonadError e m => MonadError e (RedisTraceT m)

instance MonadBaseControl b m => MonadBaseControl b (RedisTraceT m) where
  type StM (RedisTraceT m) a = StM m a

  liftBaseWith f = RedisTraceT $ liftBaseWith $ \runInBase ->
                    f (\(RedisTraceT m) -> runInBase m)

  restoreM stm = RedisTraceT (restoreM stm)

instance MonadTrans RedisTraceT where
  lift = RedisTraceT . lift . lift

instance MonadIO m => MonadRedis (RedisTraceT m) where
  redisConn = RedisTraceT $ lift $ redisConn

instance MonadIO m => MonadTraceSink (RedisTraceT m) where
  traceSink key = RedisTraceT . lift . traceSink key

instance MonadIO m => MonadTrace (RedisTraceT m) where
  traceLogs = RedisTraceT . traceLogs
  withTraceSettings f = RedisTraceT . (withTraceSettings f) . unRedisTraceT

mapRedisTraceT :: (m a -> n b) -> RedisTraceT m a -> RedisTraceT n b
mapRedisTraceT f = RedisTraceT . mapTraceT (mapRedisT f) . unRedisTraceT

runRedisTraceT :: (MonadCatch m, MonadIO m)
               => Connection
               -> TraceKey
               -> Integer
               -> RedisTraceT m a
               -> m (TraceKey, m a)
runRedisTraceT conn prefix expirySeconds (RedisTraceT tracet) =
    run $ do
      key <- lift $ newRedisTraceKey prefix expirySeconds
      pure (key, run $ withTraceKey (const key) tracet)
  where
    run :: (MonadCatch m, MonadIO m) => TraceT (RedisT m) a -> m a
    run = flip runRedisT conn . runTraceT
    

execRedisTraceT :: (MonadCatch m, MonadIO m)
                => Connection
                -> TraceKey
                -> Integer
                -> RedisTraceT m a
                -> m a
execRedisTraceT conn prefix expirySeconds =
  join . fmap snd . runRedisTraceT conn prefix expirySeconds

