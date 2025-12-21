use futures::Stream;
use numaflow_pb::clients::map::{self, MapRequest, MapResponse, map_client::MapClient};
use std::pin::Pin;
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, transport::Channel};

pub type MapStream = Pin<Box<dyn Stream<Item = Result<MapResponse, Status>> + Send>>;

#[tonic::async_trait]
pub trait MapUdfClient: Send + Sync + 'static {
    async fn map(
        &self,
        request: Request<ReceiverStream<MapRequest>>,
    ) -> Result<Response<MapStream>, Status>;

    async fn wait_until_ready(
        &self,
        request: Request<()>,
    ) -> Result<Response<map::ReadyResponse>, Status>;
}

#[tonic::async_trait]
impl MapUdfClient for MapClient<Channel> {
    async fn map(
        &self,
        request: Request<ReceiverStream<MapRequest>>,
    ) -> Result<Response<MapStream>, Status> {
        let mut me = self.clone();
        let resp = me.map_fn(request).await?;
        Ok(resp.map(|s| Box::pin(s) as MapStream))
    }

    async fn wait_until_ready(
        &self,
        request: Request<()>,
    ) -> Result<Response<map::ReadyResponse>, Status> {
        let mut me = self.clone();
        me.is_ready(request).await
    }
}

#[tonic::async_trait]
impl<T: MapUdfClient + ?Sized> MapUdfClient for Arc<T> {
    async fn map(
        &self,
        request: Request<ReceiverStream<MapRequest>>,
    ) -> Result<Response<MapStream>, Status> {
        (**self).map(request).await
    }

    async fn wait_until_ready(
        &self,
        request: Request<()>,
    ) -> Result<Response<map::ReadyResponse>, Status> {
        (**self).wait_until_ready(request).await
    }
}
