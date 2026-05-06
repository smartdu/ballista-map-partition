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

/// Encode a list of prost Messages as length-delimited bytes.
fn encode_length_delimited_list<T: prost::Message>(items: &[T]) -> Result<Vec<u8>, DataFusionError> {
    let mut buf = Vec::new();
    for item in items {
        item.encode_length_delimited(&mut buf)
            .map_err(|e| DataFusionError::Internal(format!("failed to encode expr list: {e}")))?;
    }
    Ok(buf)
}

/// Decode length-delimited bytes into a list of prost Messages.
fn decode_length_delimited_list<T: prost::Message + Default>(bytes: &[u8]) -> Result<Vec<T>, DataFusionError> {
    let mut items = Vec::new();
    let mut buf = bytes;
    while !buf.is_empty() {
        let item = T::decode_length_delimited(buf)
            .map_err(|e| DataFusionError::Internal(format!("failed to decode expr list: {e}")))?;
        let consumed = item.encoded_len() + prost::length_delimiter_len(item.encoded_len());
        buf = &buf[consumed.min(buf.len())..];
        items.push(item);
    }
    Ok(items)
}

#[derive(Debug, Default)]
pub struct ExtendedBallistaLogicalCodec {
    inner: BallistaLogicalExtensionCodec,
}

impl LogicalExtensionCodec for ExtendedBallistaLogicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[datafusion::logical_expr::LogicalPlan],
        ctx: &TaskContext,
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

                // Decode hash_partition
                let hash_partition = if mp.hash_partition_count > 0 {
                    let expr_nodes = decode_length_delimited_list::<datafusion_proto::protobuf::LogicalExprNode>(&mp.hash_partition_exprs)?;
                    let exprs: Vec<datafusion::prelude::Expr> = expr_nodes
                        .iter()
                        .map(|node| {
                            datafusion_proto::logical_plan::from_proto::parse_expr(node, ctx, self)
                        })
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|e| DataFusionError::Internal(format!("failed to parse hash partition exprs: {e}")))?;
                    Some((exprs, mp.hash_partition_count as usize))
                } else {
                    None
                };

                // Decode distribute_by
                let distribute_by = if !mp.distribute_by_expr.is_empty() {
                    let expr_node = datafusion_proto::protobuf::LogicalExprNode::decode_length_delimited(&mp.distribute_by_expr[..])
                        .map_err(|e| DataFusionError::Internal(format!("failed to decode distribute_by expr: {e}")))?;
                    Some(datafusion_proto::logical_plan::from_proto::parse_expr(&expr_node, ctx, self)
                        .map_err(|e| DataFusionError::Internal(format!("failed to parse distribute_by expr: {e}")))?)
                } else {
                    None
                };

                let num_partitions = if mp.num_partitions > 0 {
                    Some(mp.num_partitions as usize)
                } else {
                    None
                };

                let node = Arc::new(MapPartition::new(
                    mp.so_path,
                    mp.fn_name,
                    df_schema_ref,
                    input,
                    distribute_by,
                    num_partitions,
                    hash_partition,
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
            distribute_by,
            num_partitions,
            hash_partition,
            ..
        }) = node.node.as_any().downcast_ref::<MapPartition>()
        {
            let arrow_schema: datafusion::arrow::datatypes::Schema =
                output_schema.as_arrow().clone();
            let schema_bytes = encode_schema_to_ipc(&arrow_schema)?;

            // Encode hash_partition
            let (hash_partition_exprs, hash_partition_count) = match hash_partition {
                Some((exprs, count)) => {
                    let nodes: Vec<datafusion_proto::protobuf::LogicalExprNode> = exprs
                        .iter()
                        .map(|e| datafusion_proto::logical_plan::to_proto::serialize_expr(e, self))
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|e| DataFusionError::Internal(format!("failed to serialize hash partition exprs: {e}")))?;
                    let bytes = encode_length_delimited_list(&nodes)?;
                    (bytes, *count as u64)
                }
                None => (Vec::new(), 0),
            };

            // Encode distribute_by
            let distribute_by_expr = match distribute_by {
                Some(expr) => {
                    let node = datafusion_proto::logical_plan::to_proto::serialize_expr(expr, self)
                        .map_err(|e| DataFusionError::Internal(format!("failed to serialize distribute_by expr: {e}")))?;
                    let mut buf = Vec::new();
                    node.encode_length_delimited(&mut buf)
                        .map_err(|e| DataFusionError::Internal(format!("failed to encode distribute_by expr: {e}")))?;
                    buf
                }
                None => Vec::new(),
            };

            let message = LMessage {
                extension: Some(super::messages::l_message::Extension::MapPartition(
                    LMapPartition {
                        so_path: so_path.clone(),
                        fn_name: fn_name.clone(),
                        output_schema: schema_bytes,
                        hash_partition_exprs,
                        hash_partition_count,
                        distribute_by_expr,
                        num_partitions: num_partitions.unwrap_or(0) as u64,
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

    fn try_decode_file_format(
        &self,
        buf: &[u8],
        ctx: &TaskContext,
    ) -> datafusion::error::Result<Arc<dyn datafusion::datasource::file_format::FileFormatFactory>> {
        self.inner.try_decode_file_format(buf, ctx)
    }

    fn try_encode_file_format(
        &self,
        buf: &mut Vec<u8>,
        node: Arc<dyn datafusion::datasource::file_format::FileFormatFactory>,
    ) -> datafusion::error::Result<()> {
        self.inner.try_encode_file_format(buf, node)
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
        ctx: &TaskContext,
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

                // Decode hash_partition
                let hash_partition = if mp.hash_partition_count > 0 {
                    let input_schema = input.schema();
                    let expr_nodes = decode_length_delimited_list::<datafusion_proto::protobuf::PhysicalExprNode>(&mp.hash_partition_exprs)?;
                    let phys_exprs: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>> = expr_nodes
                        .iter()
                        .map(|node| {
                            datafusion_proto::physical_plan::from_proto::parse_physical_expr(
                                node, ctx, &input_schema, self,
                            )
                        })
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|e| DataFusionError::Internal(format!("failed to parse physical hash partition exprs: {e}")))?;
                    Some((phys_exprs, mp.hash_partition_count as usize))
                } else {
                    None
                };

                // Decode distribute_by
                let distribute_by = if !mp.distribute_by_expr.is_empty() {
                    let input_schema = input.schema();
                    let expr_node = datafusion_proto::protobuf::PhysicalExprNode::decode_length_delimited(&mp.distribute_by_expr[..])
                        .map_err(|e| DataFusionError::Internal(format!("failed to decode distribute_by expr: {e}")))?;
                    Some(datafusion_proto::physical_plan::from_proto::parse_physical_expr(
                        &expr_node, ctx, &input_schema, self,
                    ).map_err(|e| DataFusionError::Internal(format!("failed to parse distribute_by expr: {e}")))?)
                } else {
                    None
                };

                let num_partitions = mp.num_partitions as usize;

                let node = MapPartitionExec::new(
                    mp.so_path,
                    mp.fn_name,
                    output_schema,
                    input,
                    distribute_by,
                    num_partitions,
                    hash_partition,
                );

                Ok(Arc::new(node))
            }
            Some(super::messages::p_message::Extension::Opaque(opaque)) => {
                self.inner.try_decode(&opaque, inputs, ctx)
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
            distribute_by,
            num_partitions,
            hash_partition,
            ..
        }) = node.as_any().downcast_ref::<MapPartitionExec>()
        {
            let schema_bytes = encode_schema_to_ipc(output_schema)?;

            // Encode hash_partition
            let (hash_partition_exprs, hash_partition_count) = match hash_partition {
                Some((exprs, count)) => {
                    let nodes: Vec<datafusion_proto::protobuf::PhysicalExprNode> = exprs
                        .iter()
                        .map(|e| datafusion_proto::physical_plan::to_proto::serialize_physical_expr(e, self))
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|e| DataFusionError::Internal(format!("failed to serialize physical hash partition exprs: {e}")))?;
                    let bytes = encode_length_delimited_list(&nodes)?;
                    (bytes, *count as u64)
                }
                None => (Vec::new(), 0),
            };

            // Encode distribute_by
            let distribute_by_expr = match distribute_by {
                Some(expr) => {
                    let node = datafusion_proto::physical_plan::to_proto::serialize_physical_expr(expr, self)
                        .map_err(|e| DataFusionError::Internal(format!("failed to serialize distribute_by expr: {e}")))?;
                    let mut buf = Vec::new();
                    node.encode_length_delimited(&mut buf)
                        .map_err(|e| DataFusionError::Internal(format!("failed to encode distribute_by expr: {e}")))?;
                    buf
                }
                None => Vec::new(),
            };

            let message = PMessage {
                extension: Some(super::messages::p_message::Extension::MapPartition(
                    PMapPartition {
                        so_path: so_path.clone(),
                        fn_name: fn_name.clone(),
                        output_schema: schema_bytes,
                        hash_partition_exprs,
                        hash_partition_count,
                        distribute_by_expr,
                        num_partitions: *num_partitions as u64,
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
