from data_dallion_framework.GoldScripts.TransformationScripts import (
    # CustomTransformation,
    SingleTransformation,
    UnionTransformation,
    JoinTransformation,
)
from data_dallion_framework.Common import OrchestrationProcess


class PerformTransformation:
    def __init__(self, spark, process_id, dataset_id):
        with OrchestrationProcess.OrchestrationProcess() as orch_process:
            transformation_depedencies = (
                orch_process.get_transformation_dependency_master(
                    process_id=process_id, dataset_id=dataset_id
                )
            )

        if transformation_depedencies[0].transformation_type == "SINGLE":
            SingleTransformation.SingleTransformation(
                spark=spark,
                process_id=process_id,
                dataset_id=dataset_id,
                transformation_depedencies=transformation_depedencies,
            )

        elif transformation_depedencies[0].transformation_type == "JOIN":
            JoinTransformation.JoinTransformation(
                spark=spark,
                process_id=process_id,
                dataset_id=dataset_id,
                transformation_depedencies=transformation_depedencies,
            )

        # elif transformation_depedencies[0].transformation_type == "CUSTOM":
        #     CustomTransformation.CustomTransformation(
        #         spark=spark,
        #         process_id=process_id,
        #         dataset_id=dataset_id,
        #         transformation_depedencies=transformation_depedencies,
        #     )

        elif transformation_depedencies[0].transformation_type == "UNION":
            UnionTransformation.UnionTransformation(
                spark=spark,
                process_id=process_id,
                dataset_id=dataset_id,
                transformation_depedencies=transformation_depedencies,
            )
