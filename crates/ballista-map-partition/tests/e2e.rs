#[cfg(test)]
mod test {
    use std::sync::Arc;

    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::SessionContext;
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
        let result = df.map_partition("/tmp/test.so", "my_proc", Arc::new(output_schema))?;

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
        let result = df.map_partition("/tmp/test.so", "my_proc", Arc::new(output_schema))?;

        let plan = result.create_physical_plan().await?;
        let bytes = physical_plan_to_bytes_with_extension_codec(plan.clone(), &codec)?;
        let new_plan =
            physical_plan_from_bytes_with_extension_codec(&bytes, &ctx.task_ctx(), &codec)?;

        let plan_formatted = format!("{}", displayable(plan.as_ref()).indent(false));
        let new_plan_formatted = format!("{}", displayable(new_plan.as_ref()).indent(false));

        assert_eq!(plan_formatted, new_plan_formatted);

        Ok(())
    }
}
