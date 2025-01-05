import { BIGTABLE } from "../../../common/bigtable";
import { eqData } from "../../../common/bigtable_data_matcher";
import { ProcessMonthlyMeterReadingHandler } from "./process_monthly_meter_reading_handler";
import {
  GENERATE_EARNINGS_STATEMENT_REQUEST_BODY,
  MeterType,
} from "@phading/commerce_service_interface/node/publisher/interface";
import { eqMessage } from "@selfage/message/test_matcher";
import { NodeServiceClientMock } from "@selfage/node_service_client/client_mock";
import { assertThat, eq } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

TEST_RUNNER.run({
  name: "ProcessMonthlyMeterReadingHandlerTest",
  cases: [
    {
      name: "Process",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "t5#2024-10#publisher1",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
          {
            key: "d5#2024-10#publisher1#01",
            data: {
              t: {
                ws: {
                  value: 12,
                },
                nk: {
                  value: 4200,
                },
              },
            },
          },
          {
            key: "d5#2024-10#publisher1#20",
            data: {
              t: {
                uk: {
                  value: 8000,
                },
                smm: {
                  value: 56000,
                },
              },
            },
          },
          {
            key: "d5#2024-10#publisher1#30",
            data: {
              t: {
                ws: {
                  value: 1300,
                },
                nk: {
                  value: 1400,
                },
                uk: {
                  value: 88000,
                },
                smm: {
                  value: 96000,
                },
              },
            },
          },
        ]);
        let clientMock = new NodeServiceClientMock();
        let handler = new ProcessMonthlyMeterReadingHandler(
          BIGTABLE,
          clientMock,
        );

        // Execute
        await handler.handle("", {
          rowKey: "t5#2024-10#publisher1",
        });

        // Verify
        assertThat(
          (await BIGTABLE.row("f4#publisher1#2024-10").get())[0].data,
          eqData({
            t: {
              ws: {
                value: 1312,
              },
              nm: {
                value: 7,
              },
              um: {
                value: 94,
              },
              smh: {
                value: 2534,
              },
            },
          }),
          "final publisher data",
        );
        assertThat(
          clientMock.request.body,
          eqMessage(
            {
              accountId: "publisher1",
              month: "2024-10",
              readings: [
                {
                  meterType: MeterType.SHOW_WATCH_TIME_SEC,
                  reading: 1312,
                },
                {
                  meterType: MeterType.NETWORK_TRANSMITTED_MB,
                  reading: 7,
                },
                {
                  meterType: MeterType.UPLOADED_MB,
                  reading: 94,
                },
                {
                  meterType: MeterType.STORAGE_MB_HOUR,
                  reading: 2534,
                },
              ],
            },
            GENERATE_EARNINGS_STATEMENT_REQUEST_BODY,
          ),
          "generate earnings request",
        );
        assertThat(
          (await BIGTABLE.row("t5#2024-10#publisher1").exists())[0],
          eq(false),
          "task deleted",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("t");
        await BIGTABLE.deleteRows("d");
        await BIGTABLE.deleteRows("f");
      },
    },
    {
      name: "OnlyUploadAndStorage",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "t5#2024-10#publisher1",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
          {
            key: "d5#2024-10#publisher1#01",
            data: {
              t: {
                uk: {
                  value: 8000,
                },
                smm: {
                  value: 56000,
                },
              },
            },
          },
        ]);
        let clientMock = new NodeServiceClientMock();
        let handler = new ProcessMonthlyMeterReadingHandler(
          BIGTABLE,
          clientMock,
        );

        // Execute
        await handler.handle("", {
          rowKey: "t5#2024-10#publisher1",
        });

        // Verify
        assertThat(
          (await BIGTABLE.row("f4#publisher1#2024-10").get())[0].data,
          eqData({
            t: {
              um: {
                value: 8,
              },
              smh: {
                value: 934,
              },
            },
          }),
          "final publisher data",
        );
        assertThat(
          clientMock.request.body,
          eqMessage(
            {
              accountId: "publisher1",
              month: "2024-10",
              readings: [
                {
                  meterType: MeterType.SHOW_WATCH_TIME_SEC,
                  reading: 0,
                },
                {
                  meterType: MeterType.NETWORK_TRANSMITTED_MB,
                  reading: 0,
                },
                {
                  meterType: MeterType.UPLOADED_MB,
                  reading: 8,
                },
                {
                  meterType: MeterType.STORAGE_MB_HOUR,
                  reading: 934,
                },
              ],
            },
            GENERATE_EARNINGS_STATEMENT_REQUEST_BODY,
          ),
          "generate earnings request",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("t");
        await BIGTABLE.deleteRows("d");
        await BIGTABLE.deleteRows("f");
      },
    },
    {
      name: "OnlyWatchTimeAndNetwork",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "t5#2024-10#publisher1",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
          {
            key: "d5#2024-10#publisher1#01",
            data: {
              t: {
                ws: {
                  value: 12,
                },
                nk: {
                  value: 4200,
                },
              },
            },
          },
        ]);
        let clientMock = new NodeServiceClientMock();
        let handler = new ProcessMonthlyMeterReadingHandler(
          BIGTABLE,
          clientMock,
        );

        // Execute
        await handler.handle("", {
          rowKey: "t5#2024-10#publisher1",
        });

        // Verify
        assertThat(
          (await BIGTABLE.row("f4#publisher1#2024-10").get())[0].data,
          eqData({
            t: {
              ws: {
                value: 12,
              },
              nm: {
                value: 5,
              },
            },
          }),
          "final publisher data",
        );
        assertThat(
          clientMock.request.body,
          eqMessage(
            {
              accountId: "publisher1",
              month: "2024-10",
              readings: [
                {
                  meterType: MeterType.SHOW_WATCH_TIME_SEC,
                  reading: 12,
                },
                {
                  meterType: MeterType.NETWORK_TRANSMITTED_MB,
                  reading: 5,
                },
                {
                  meterType: MeterType.UPLOADED_MB,
                  reading: 0,
                },
                {
                  meterType: MeterType.STORAGE_MB_HOUR,
                  reading: 0,
                },
              ],
            },
            GENERATE_EARNINGS_STATEMENT_REQUEST_BODY,
          ),
          "generate earnings request",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("t");
        await BIGTABLE.deleteRows("d");
        await BIGTABLE.deleteRows("f");
      },
    },
  ],
});
