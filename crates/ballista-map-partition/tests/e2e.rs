#[cfg(test)]
mod test {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::{SessionContext, col};
    use datafusion_proto::bytes::{
        logical_plan_from_bytes_with_extension_codec, logical_plan_to_bytes_with_extension_codec,
        physical_plan_from_bytes_with_extension_codec, physical_plan_to_bytes_with_extension_codec,
    };

    use ballista_map_partition::{
        codec::extension::{ExtendedBallistaLogicalCodec, ExtendedBallistaPhysicalCodec},
        dataframe::map_partition::DataFrameExt,
        planner::extension_planner::QueryPlannerWithExtensions,
    };

    fn context() -> SessionContext {
        let query_planner = Arc::new(QueryPlannerWithExtensions::default());

        let state = SessionStateBuilder::new()
            .with_query_planner(query_planner)
            .with_default_features()
            .build();

        SessionContext::new_with_state(state)
    }

    #[tokio::test]
    async fn should_validate_empty_so_path() -> datafusion::error::Result<()> {
        let ctx = context();
        let df = ctx
            .sql("select unnest([1, 2, 3]) as a")
            .await?;

        let output_schema = df.schema().as_arrow().clone();
        assert!(df.map_partition("", "test", Arc::new(output_schema)).is_err());

        Ok(())
    }

    #[tokio::test]
    async fn should_validate_empty_fn_name() -> datafusion::error::Result<()> {
        let ctx = context();
        let df = ctx
            .sql("select unnest([1, 2, 3]) as a")
            .await?;

        let output_schema = df.schema().as_arrow().clone();
        assert!(df.map_partition("/path/to.so", "", Arc::new(output_schema)).is_err());

        Ok(())
    }

    #[tokio::test]
    async fn should_round_trip_logical_plan() -> datafusion::error::Result<()> {
        let ctx = context();
        let codec = ExtendedBallistaLogicalCodec::default();
        let df = ctx
            .sql("select unnest([1, 2, 3]) as a")
            .await?;

        let output_schema = df.schema().as_arrow().clone();
        let result = df.map_partition("/tmp/test.so", "my_proc", Arc::new(output_schema))?.build()?;

        let plan = result.logical_plan();
        let bytes = logical_plan_to_bytes_with_extension_codec(plan, &codec)?;
        let new_plan =
            logical_plan_from_bytes_with_extension_codec(&bytes, &ctx.task_ctx(), &codec)?;

        assert_eq!(plan, &new_plan);

        Ok(())
    }

    #[tokio::test]
    async fn should_round_trip_physical_plan() -> datafusion::error::Result<()> {
        let ctx = context();
        let codec = ExtendedBallistaPhysicalCodec::default();
        let df = ctx
            .sql("select unnest([1, 2, 3]) as a")
            .await?;

        let output_schema = df.schema().as_arrow().clone();
        let result = df.map_partition("/tmp/test.so", "my_proc", Arc::new(output_schema))?.build()?;

        let plan = result.create_physical_plan().await?;
        let bytes = physical_plan_to_bytes_with_extension_codec(plan.clone(), &codec)?;
        let new_plan =
            physical_plan_from_bytes_with_extension_codec(&bytes, &ctx.task_ctx(), &codec)?;

        let plan_formatted = format!("{}", displayable(plan.as_ref()).indent(false));
        let new_plan_formatted = format!("{}", displayable(new_plan.as_ref()).indent(false));

        assert_eq!(plan_formatted, new_plan_formatted);

        Ok(())
    }

    #[tokio::test]
    async fn should_auto_repartition_with_distribute_by() -> datafusion::error::Result<()> {
        let query_planner = Arc::new(QueryPlannerWithExtensions::default());

        // Use distribute_by with num_partitions >= distinct values
        let num_partitions = 10;

        let config = datafusion::prelude::SessionConfig::new()
            .with_target_partitions(num_partitions);

        let state = SessionStateBuilder::new()
            .with_query_planner(query_planner)
            .with_default_features()
            .with_config(config)
            .build();

        let ctx = SessionContext::new_with_state(state);
        let df = ctx
            .sql("select unnest(['east', 'east', 'west', 'west', 'north']) as region, unnest(['ch1', 'ch1', 'ch2', 'ch2', 'ch3']) as channelid, unnest(['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05']) as captime, unnest(['r1', 'r2', 'r3', 'r4', 'r5']) as recordid")
            .await?;

        let output_schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("dossierid", DataType::Utf8, false),
            Field::new("recordids", DataType::Utf8, false),
        ]));

        let so_path = std::env::var("MAP_PARTITION_SO").unwrap_or_default();
        if so_path.is_empty() {
            return Ok(());
        }

        // Use with_distribute_by — same value → same processor, different values → different processors
        let df = df
            .map_partition(&so_path, "region_cluster_processor", output_schema)?
            .with_distribute_by(col("region"), num_partitions)?
            .build()?;

        // Verify physical plan contains RepartitionExec
        let plan = df.clone().create_physical_plan().await?;
        let plan_str = format!("{}", displayable(plan.as_ref()).indent(false));
        eprintln!("Physical plan:\n{plan_str}");
        assert!(
            plan_str.contains("RepartitionExec"),
            "Expected RepartitionExec in plan, got:\n{plan_str}"
        );

        // Execute and collect results
        let batches = df.collect().await?;

        // Check no CROSS_REGION_ERROR
        for batch in &batches {
            let region_col = batch.column(0);
            let cluster_col = batch.column(1);
            let regions = region_col.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
            let clusters = cluster_col.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
            for i in 0..batch.num_rows() {
                let cluster = clusters.value(i);
                assert_ne!(
                    cluster, "CROSS_REGION_ERROR",
                    "Found CROSS_REGION_ERROR at row {i} with region={}",
                    regions.value(i)
                );
            }
        }

        Ok(())
    }
}
