import { BIGTABLE } from "../../../common/bigtable";
import { eqData } from "../../../common/bigtable_data_matcher";
import { ProcessMonthlyMeterReadingHandler } from "./process_monthly_meter_reading_handler";
import {
  GENERATE_EARNINGS_STATEMENT,
  GENERATE_EARNINGS_STATEMENT_REQUEST_BODY,
  GenerateEarningsStatementResponse,
  MeterType,
} from "@phading/commerce_service_interface/backend/publisher/interface";
import {
  GET_STORAGE_METER_READING,
  GET_STORAGE_METER_READING_REQUEST_BODY,
  GET_UPLOAD_METER_READING,
  GET_UPLOAD_METER_READING_REQUEST_BODY,
  GetStorageMeterReadingResponse,
  GetUploadMeterReadingResponse,
} from "@phading/product_service_interface/show/backend/interface";
import { eqMessage } from "@selfage/message/test_matcher";
import { NodeServiceClientMock } from "@selfage/node_service_client/client_mock";
import {
  assertReject,
  assertThat,
  containStr,
  eq,
} from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

async function initData() {
  await BIGTABLE.insert([
    {
      key: "t7#2024-10#publisher1",
      data: {
        t: {
          w: {
            value: 0,
          },
          mb: {
            value: 0,
          },
        },
        c: {
          p: {
            value: "",
          },
        },
      },
    },
    {
      key: "t5#2024-10#publisher1#01",
      data: {
        t: {
          w: {
            value: 12,
          },
          kb: {
            value: 4200,
          },
        },
      },
    },
    {
      key: "t5#2024-10#publisher1#20",
      data: {
        t: {
          w: {
            value: 100,
          },
          kb: {
            value: 120000,
          },
        },
      },
    },
    {
      key: "t5#2024-10#publisher2#01",
      data: {
        t: {
          w: {
            value: 1300,
          },
          kb: {
            value: 1400,
          },
        },
      },
    },
  ]);
}

TEST_RUNNER.run({
  name: "ProcessMonthlyMeterReadingHandlerTest",
  cases: [
    {
      name: "ProcessedInOneShot",
      execute: async () => {
        // Prepare
        await initData();
        let startTimeMs = 1727769600000; // 2024-10-01
        let endTimeMs = 1730448000000; // 2024-11-01
        let clientMock = new (class extends NodeServiceClientMock {
          public async send(request: any): Promise<any> {
            switch (request.descriptor) {
              case GET_STORAGE_METER_READING:
                assertThat(
                  request.body,
                  eqMessage(
                    {
                      accountId: "publisher1",
                      startTimeMs,
                      endTimeMs,
                    },
                    GET_STORAGE_METER_READING_REQUEST_BODY,
                  ),
                  "get storage meter reading request",
                );
                return {
                  mbh: 3334,
                } as GetStorageMeterReadingResponse;
              case GET_UPLOAD_METER_READING:
                assertThat(
                  request.body,
                  eqMessage(
                    {
                      accountId: "publisher1",
                      startTimeMs,
                      endTimeMs,
                    },
                    GET_UPLOAD_METER_READING_REQUEST_BODY,
                  ),
                  "get upload meter reading request",
                );
                return {
                  mb: 8899,
                } as GetUploadMeterReadingResponse;
              case GENERATE_EARNINGS_STATEMENT:
                this.request = request;
                return {} as GenerateEarningsStatementResponse;
              default:
                throw new Error("Unexpected");
            }
          }
        })();
        let handler = new ProcessMonthlyMeterReadingHandler(
          BIGTABLE,
          clientMock,
        );

        // Execute
        await handler.handle("", {
          rowKey: "t7#2024-10#publisher1",
        });

        // Verify
        assertThat(
          (await BIGTABLE.row("f4#publisher1#2024-10").get())[0].data,
          eqData({
            t: {
              w: {
                value: 112,
              },
              mb: {
                value: 123,
              },
              smbh: {
                value: 3334,
              },
              umb: {
                value: 8899,
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
                  reading: 112,
                },
                {
                  meterType: MeterType.NETWORK_TRANSMITTED_MB,
                  reading: 123,
                },
                {
                  meterType: MeterType.STORAGE_MB_HOUR,
                  reading: 3334,
                },
                {
                  meterType: MeterType.UPLOAD_MB,
                  reading: 8899,
                },
              ],
            },
            GENERATE_EARNINGS_STATEMENT_REQUEST_BODY,
          ),
          "generate earnings request",
        );
        assertThat(
          (await BIGTABLE.row("t7#2024-10#publisher1").exists())[0],
          eq(false),
          "original row deleted",
        );
        assertThat(
          (await BIGTABLE.row("t5#2024-10#publisher1#01").exists())[0],
          eq(false),
          "one data row deleted",
        );
        assertThat(
          (await BIGTABLE.row("t5#2024-10#publisher2#01").exists())[0],
          eq(true),
          "extra data row exists",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("t");
        await BIGTABLE.deleteRows("f");
      },
    },
    {
      name: "InterruptAfterCheckPoint_ResumeAndMarkDone_ResumeWithNoAction",
      execute: async () => {
        // Prepare
        await initData();
        let clientMock = new (class extends NodeServiceClientMock {
          public async send(request: any): Promise<any> {
            switch (request.descriptor) {
              case GET_STORAGE_METER_READING:
                return {
                  mbh: 3334,
                } as GetStorageMeterReadingResponse;
              case GET_UPLOAD_METER_READING:
                return {
                  mb: 8899,
                } as GetUploadMeterReadingResponse;
              case GENERATE_EARNINGS_STATEMENT:
                this.request = request;
                return {} as GenerateEarningsStatementResponse;
              default:
                throw new Error("Unexpected");
            }
          }
        })();
        let handler = new ProcessMonthlyMeterReadingHandler(
          BIGTABLE,
          clientMock,
        );
        handler.interruptAfterCheckPoint = () => {
          throw new Error("fake error");
        };

        // Execute
        let error = await assertReject(
          handler.handle("", {
            rowKey: "t7#2024-10#publisher1",
          }),
        );

        // Verify
        assertThat(error.message, containStr("fake"), "error");
        assertThat(
          (await BIGTABLE.row("t7#2024-10#publisher1").get())[0].data,
          eqData({
            t: {
              w: {
                value: 112,
              },
              mb: {
                value: 123,
              },
            },
            c: {
              p: {
                value: "1",
              },
            },
          }),
          "checkpoint data",
        );
        assertThat(clientMock.request, eq(undefined), "no RC");

        // Execute
        await handler.handle("", {
          rowKey: "t7#2024-10#publisher1",
        });

        // Verify
        assertThat(
          (await BIGTABLE.row("f4#publisher1#2024-10").get())[0].data,
          eqData({
            t: {
              w: {
                value: 112,
              },
              mb: {
                value: 123,
              },
              smbh: {
                value: 3334,
              },
              umb: {
                value: 8899,
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
                  reading: 112,
                },
                {
                  meterType: MeterType.NETWORK_TRANSMITTED_MB,
                  reading: 123,
                },
                {
                  meterType: MeterType.STORAGE_MB_HOUR,
                  reading: 3334,
                },
                {
                  meterType: MeterType.UPLOAD_MB,
                  reading: 8899,
                },
              ],
            },
            GENERATE_EARNINGS_STATEMENT_REQUEST_BODY,
          ),
          "generate earnings request",
        );
        assertThat(
          (await BIGTABLE.row("t7#2024-10#publisher1").exists())[0],
          eq(false),
          "original row deleted",
        );
        assertThat(
          (await BIGTABLE.row("t5#2024-10#publisher1#01").exists())[0],
          eq(false),
          "one data row deleted",
        );
        assertThat(
          (await BIGTABLE.row("t5#2024-10#publisher2#01").exists())[0],
          eq(true),
          "extra data row exists",
        );

        // Execute
        await handler.handle("", {
          rowKey: "t7#2024-10#publisher1",
        });

        // Verify no error and no actions
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("t");
        await BIGTABLE.deleteRows("f");
      },
    },
    {
      name: "SimulatedWriteConflict",
      execute: async () => {
        // Prepare
        await initData();
        let clientMock = new NodeServiceClientMock();
        let handler = new ProcessMonthlyMeterReadingHandler(
          BIGTABLE,
          clientMock,
        );
        handler.interfereBeforeCheckPoint = async () => {
          await BIGTABLE.insert([
            {
              key: "t7#2024-10#publisher1",
              data: {
                t: {
                  w: {
                    value: 100,
                  },
                },
                c: {
                  p: {
                    value: "1",
                  },
                },
              },
            },
          ]);
        };

        // Execute
        let error = await assertReject(
          handler.handle("", {
            rowKey: "t7#2024-10#publisher1",
          }),
        );

        // Verify
        assertThat(
          error.message,
          containStr("Row t7#2024-10#publisher1 is already completed"),
          "error",
        );
        assertThat(
          (await BIGTABLE.row("t7#2024-10#publisher1").get())[0].data,
          eqData({
            t: {
              w: {
                value: 100,
              },
              mb: {
                value: 0,
              },
            },
            c: {
              p: {
                value: "1",
              },
            },
          }),
          "checkpoint data",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("t");
        await BIGTABLE.deleteRows("f");
      },
    },
  ],
});
