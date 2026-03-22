from app.celery.celery_app import celery_app
from app.logging.logger import setup_logger
from app.utils.wrapper_utils import log_task_run
from app.services.kafka_metric_service import KafkaMetricService


logger = setup_logger(__name__)


@celery_app.task(
    name="app.tasks.common_task.collect_kafka_metrics",
    time_limit=300,  # 建议比 consume 短一点
)
@log_task_run(
    task_name="collect_kafka_metrics",
    task_type="monitor",
)
def collect_kafka_metrics() -> None:
    """
    Collect Kafka cluster + topic metrics and store into ClickHouse.
    """
    logger.info("Start task: collect_kafka_metrics")

    try:
        service = KafkaMetricService()
        service.collect_all()

    except Exception as e:
        logger.exception(
            "Error in collect_kafka_metrics: %s",
            str(e),
        )
    logger.info("Finish task: collect_kafka_metrics")
