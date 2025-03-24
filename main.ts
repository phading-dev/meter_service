import http = require("http");
import { ENV_VARS } from "./env_vars";
import { GetDailyBatchHandler as ConsumerGetDailyBatchHandler } from "./show/node/consumer/get_daily_batch_handler";
import { GetMonthlyBatchHandler as ConsumerGetMonthlyBatchHandler } from "./show/node/consumer/get_monthly_batch_handler";
import { ProcessDailyMeterReadingHandler as ConsumerProcessDailyMeterReadingHandler } from "./show/node/consumer/process_daily_meter_reading_handler";
import { ProcessMonthlyMeterReadingHandler as ConsumerProcessMonthlyMeterReadingHandler } from "./show/node/consumer/process_monthly_meter_reading_handler";
import { GetDailyStorageBatchHandler as PublisherGetDailyStorageBatchHandler } from "./show/node/publisher/get_daily_storage_batch_handler";
import { GetDailyWatchBatchHandler as PublisherGetDailyWatchBatchHandler } from "./show/node/publisher/get_daily_watch_batch_handler";
import { GetMonthlyBatchHandler as PublisherGetMonthlyBatchHandler } from "./show/node/publisher/get_monthly_batch_handler";
import { ProcessDailyStorageReadingHandler as PublisherProcessDailyStorageReadingHandler } from "./show/node/publisher/process_daily_storage_reading_handler";
import { ProcessDailyWatchReadingHandler as PublisherProcessDailyWatchReadingHandler } from "./show/node/publisher/process_daily_watch_reading_handler";
import { ProcessMonthlyMeterReadingHandler as PublisherProcessMonthlyMeterReadingHandler } from "./show/node/publisher/process_monthly_meter_reading_handler";
import { RecordStorageEndHandler } from "./show/node/publisher/record_storage_end_handler";
import { RecordStorageStartHandler } from "./show/node/publisher/record_storage_start_handler";
import { RecordUploadedHandler } from "./show/node/publisher/record_uploaded_handler";
import { ListMeterReadingsPerDayHandler as ConsumerListMeterReadingsPerDayHandler } from "./show/web/consumer/list_meter_reading_per_day_handler";
import { ListMeterReadingsPerMonthHandler as ConsumerListMeterReadingsPerMonthHandler } from "./show/web/consumer/list_meter_reading_per_month_handler";
import { ListMeterReadingPerSeasonHandler as ConsumerListMeterReadingPerSeasonHandler } from "./show/web/consumer/list_meter_reading_per_season_handler";
import { RecordNetworkTransmissionHandler } from "./show/web/consumer/record_network_transmission_handler";
import { RecordWatchTimeHandler } from "./show/web/consumer/record_watch_time_handler";
import { ListMeterReadingsPerDayHandler as PublisherListMeterReadingsPerDayHandler } from "./show/web/publisher/list_meter_reading_per_day_handler";
import { ListMeterReadingsPerMonthHandler as PublisherListMeterReadingsPerMonthHandler } from "./show/web/publisher/list_meter_reading_per_month_handler";
import { ListMeterReadingPerSeasonHandler as PublisherListMeterReadingPerSeasonHandler } from "./show/web/publisher/list_meter_reading_per_season_handler";
import {
  METER_NODE_SERVICE,
  METER_WEB_SERVICE,
} from "@phading/meter_service_interface/service";
import { ServiceHandler } from "@selfage/service_handler/service_handler";

async function main() {
  let service = ServiceHandler.create(
    http.createServer(),
    ENV_VARS.externalOrigin,
  )
    .addCorsAllowedPreflightHandler()
    .addHealthCheckHandler()
    .addReadinessHandler()
    .addMetricsHandler();
  service
    .addHandlerRegister(METER_NODE_SERVICE)
    .add(ConsumerGetDailyBatchHandler.create())
    .add(ConsumerGetMonthlyBatchHandler.create())
    .add(ConsumerProcessDailyMeterReadingHandler.create())
    .add(ConsumerProcessMonthlyMeterReadingHandler.create())
    .add(PublisherGetDailyStorageBatchHandler.create())
    .add(PublisherGetDailyWatchBatchHandler.create())
    .add(PublisherGetMonthlyBatchHandler.create())
    .add(PublisherProcessDailyStorageReadingHandler.create())
    .add(PublisherProcessDailyWatchReadingHandler.create())
    .add(PublisherProcessMonthlyMeterReadingHandler.create())
    .add(RecordStorageEndHandler.create())
    .add(RecordStorageStartHandler.create())
    .add(RecordUploadedHandler.create());
  service
    .addHandlerRegister(METER_WEB_SERVICE)
    .add(ConsumerListMeterReadingsPerDayHandler.create())
    .add(ConsumerListMeterReadingsPerMonthHandler.create())
    .add(ConsumerListMeterReadingPerSeasonHandler.create())
    .add(RecordNetworkTransmissionHandler.create())
    .add(RecordWatchTimeHandler.create())
    .add(PublisherListMeterReadingsPerDayHandler.create())
    .add(PublisherListMeterReadingsPerMonthHandler.create())
    .add(PublisherListMeterReadingPerSeasonHandler.create());
  await service.start(ENV_VARS.port);
}

main();
