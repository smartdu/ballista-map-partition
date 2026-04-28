use std::sync::Arc;

use ballista_core::serde::{BallistaLogicalExtensionCodec, BallistaPhysicalExtensionCodec};
use datafusion::common::plan_err;
use datafusion::common::DFSchema;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use prost::Message;

use crate::logical::map_partition::MapPartition;
use crate::physical::map_partition_exec::{MapPartitionExec, encode_schema_to_ipc};

use super::messages::{LMapPartition, LMessage, PMapPartition, PMessage};

#[derive(Debug, Default)]
pub struct ExtendedBallistaLogicalCodec {
    inner: BallistaLogicalExtensionCodec,
}

impl LogicalExtensionCodec for ExtendedBallistaLogicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[datafusion::logical_expr::LogicalPlan],
        _ctx: &TaskContext,
    ) -> datafusion::error::Result<datafusion::logical_expr::Extension> {
        let message =
            LMessage::decode(buf).map_err(|e| DataFusionError::Internal(e.to_string()))?;

        match message.extension {
            Some(super::messages::l_message::Extension::MapPartition(mp)) => {
                let input = inputs
                    .first()
                    .ok_or(DataFusionError::Plan(
                        "MapPartition expects single input".to_string(),
                    ))?
                    .clone();

                // Decode output_schema from Arrow IPC bytes
                let output_schema = decode_arrow_schema(&mp.output_schema)?;

                let df_schema = DFSchema::try_from(output_schema)
                    .map_err(|e| DataFusionError::Internal(format!("invalid schema: {e}")))?;
                let df_schema_ref = Arc::new(df_schema);

                let node = Arc::new(MapPartition::new(
                    mp.so_path,
                    mp.fn_name,
                    df_schema_ref,
                    input,
                ));

                Ok(datafusion::logical_expr::Extension { node })
            }
            None => plan_err!("Can't cast logical extension: no extension variant"),
        }
    }

    fn try_encode(
        &self,
        node: &datafusion::logical_expr::Extension,
        buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
        if let Some(MapPartition {
            so_path,
            fn_name,
            output_schema,
            ..
        }) = node.node.as_any().downcast_ref::<MapPartition>()
        {
            let arrow_schema: datafusion::arrow::datatypes::Schema =
                output_schema.as_arrow().clone();
            let schema_bytes = encode_schema_to_ipc(&arrow_schema)?;

            let message = LMessage {
                extension: Some(super::messages::l_message::Extension::MapPartition(
                    LMapPartition {
                        so_path: so_path.clone(),
                        fn_name: fn_name.clone(),
                        output_schema: schema_bytes,
                    },
                )),
            };

            message
                .encode(buf)
                .map_err(|e| DataFusionError::Internal(e.to_string()))?;

            Ok(())
        } else {
            self.inner.try_encode(node, buf)
        }
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        table_ref: &datafusion::sql::TableReference,
        schema: datafusion::arrow::datatypes::SchemaRef,
        ctx: &TaskContext,
    ) -> datafusion::error::Result<Arc<dyn datafusion::catalog::TableProvider>> {
        self.inner
            .try_decode_table_provider(buf, table_ref, schema, ctx)
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &datafusion::sql::TableReference,
        node: Arc<dyn datafusion::catalog::TableProvider>,
        buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
        self.inner.try_encode_table_provider(table_ref, node, buf)
    }
}

#[derive(Debug, Default)]
pub struct ExtendedBallistaPhysicalCodec {
    inner: BallistaPhysicalExtensionCodec,
}

impl PhysicalExtensionCodec for ExtendedBallistaPhysicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn datafusion::physical_plan::ExecutionPlan>],
        registry: &TaskContext,
    ) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        let message =
            PMessage::decode(buf).map_err(|e| DataFusionError::Internal(e.to_string()))?;

        match message.extension {
            Some(super::messages::p_message::Extension::MapPartition(mp)) => {
                let input = inputs
                    .first()
                    .ok_or(DataFusionError::Plan(
                        "MapPartition expects single input".to_string(),
                    ))?
                    .clone();

                let output_schema = decode_arrow_schema(&mp.output_schema)?;

                let node = MapPartitionExec::new(
                    mp.so_path,
                    mp.fn_name,
                    output_schema,
                    input,
                );

                Ok(Arc::new(node))
            }
            Some(super::messages::p_message::Extension::Opaque(opaque)) => {
                self.inner.try_decode(&opaque, inputs, registry)
            }
            None => plan_err!("Can't cast physical extension: no extension variant"),
        }
    }

    fn try_encode(
        &self,
        node: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
        if let Some(MapPartitionExec {
            so_path,
            fn_name,
            output_schema,
            ..
        }) = node.as_any().downcast_ref::<MapPartitionExec>()
        {
            let schema_bytes = encode_schema_to_ipc(output_schema)?;

            let message = PMessage {
                extension: Some(super::messages::p_message::Extension::MapPartition(
                    PMapPartition {
                        so_path: so_path.clone(),
                        fn_name: fn_name.clone(),
                        output_schema: schema_bytes,
                    },
                )),
            };

            message
                .encode(buf)
                .map_err(|e| DataFusionError::Internal(e.to_string()))?;

            Ok(())
        } else {
            // Fallback: encode with inner codec, wrap as Opaque
            let mut opaque = vec![];
            self.inner
                .try_encode(node, &mut opaque)
                .map_err(|e| DataFusionError::Internal(e.to_string()))?;

            let message = PMessage {
                extension: Some(super::messages::p_message::Extension::Opaque(opaque)),
            };

            message
                .encode(buf)
                .map_err(|e| DataFusionError::Internal(e.to_string()))?;

            Ok(())
        }
    }
}

/// Helper: decode Arrow IPC bytes into a SchemaRef
fn decode_arrow_schema(bytes: &[u8]) -> datafusion::error::Result<datafusion::arrow::datatypes::SchemaRef>
{
    use datafusion::arrow::ipc::reader::StreamReader;
    use std::io::Cursor;

    let reader = StreamReader::try_new(Cursor::new(bytes), None)
        .map_err(|e| DataFusionError::Internal(format!("failed to decode schema from IPC: {e}")))?;
    Ok(Arc::new((*reader.schema()).clone()))
}
