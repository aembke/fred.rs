use std::convert::TryInto;
use std::future::Future;
use std::hash::{Hash, Hasher};

use bytes_utils::Str;

use crate::error::{RedisError, RedisErrorKind};
use crate::interfaces::LuaInterface;
use crate::modules::response::FromRedis;
use crate::types::{MultipleKeys, MultipleValues};

/// Wrapper for a Redis Lua script that uses the script cache
/// to avoid sending the source on every evaluation.
#[derive(Clone, Debug)]
pub struct RedisScript {
    source: Str,
    sha1: Str,
}

impl RedisScript {
    /// Creates a new script from a string.
    ///
    /// # Examples
    ///
    /// ```
    /// use fred::util::RedisScript;
    /// let script1 = RedisScript::new("return 'Hello world'");
    /// let script2 = RedisScript::new(String::from("return 'Hello world'"));
    /// ```
    pub fn new(source: impl Into<Str>) -> Self {
        let source = source.into();
        let sha1 = crate::util::sha1_hash(&source);
        RedisScript {
            source,
            sha1: sha1.into(),
        }
    }

    /// Returns the Lua script (as passed into [`RedisScript::new`]).
    #[inline]
    pub fn source(&self) -> Str {
        self.source.clone()
    }

    /// Returns the SHA-1 hash of the Lua script.
    #[inline]
    pub fn sha(&self) -> Str {
        self.sha1.clone()
    }

    /// Loads the script without evaluating it.
    pub fn load<'a>(
        &'a self,
        client: &'a (impl LuaInterface + ?Sized),
    ) -> impl Future<Output = Result<(), RedisError>> + Send + 'a {
        async move {
            if client.client_config().server.is_clustered() {
                client.script_load_cluster(self.source()).await?;
            } else {
                client.script_load(self.source()).await?;
            }
            Ok(())
        }
    }

    /// Evaluate the script.
    pub fn eval<'a, R, K, V>(
        &'a self,
        client: &'a (impl LuaInterface + ?Sized),
        keys: K,
        args: V,
    ) -> impl Future<Output = Result<R, RedisError>> + Send + 'a
    where
        R: FromRedis + Unpin + Send + 'static,
        K: Into<MultipleKeys>,
        V: TryInto<MultipleValues>,
        V::Error: Into<RedisError>,
    {
        let keys = keys.into();
        let args_result = args.try_into().map_err(Into::into);
        async move {
            let args = match args_result {
                Ok(args) => args,
                Err(err) => return Err(err),
            };
            match client.evalsha(self.sha1.clone(), keys.clone(), args.clone()).await {
                Ok(result) => Ok(result),
                Err(err) if is_noscript_error(&err) => {
                    // Calling EVAL will load the script into the server's cache.
                    Ok(client.eval(self.source(), keys, args).await?)
                }
                Err(err) => Err(err),
            }
        }
    }
}

impl From<Str> for RedisScript {
    fn from(source: Str) -> Self {
        RedisScript::new(source)
    }
}

impl PartialEq for RedisScript {
    fn eq(&self, other: &Self) -> bool {
        self.source == other.source
    }
}

impl Eq for RedisScript {}

impl Hash for RedisScript {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(self.sha1.as_bytes())
    }
}

fn is_noscript_error(err: &RedisError) -> bool {
    err.kind() == &RedisErrorKind::Unknown && err.details().starts_with("NOSCRIPT ")
}
