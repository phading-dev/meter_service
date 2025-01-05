import { BIGTABLE } from "../../../common/bigtable";
import { eqData } from "../../../common/bigtable_data_matcher";
import { ProcessDailyMeterReadingHandler } from "./process_daily_meter_reading_handler";
import {
  GET_SEASON_GRADE,
  GET_SEASON_PUBLISHER,
  GetSeasonGradeResponse,
  GetSeasonPublisherResponse,
} from "@phading/product_service_interface/show/node/interface";
import { newNotFoundError } from "@selfage/http_error";
import { NodeServiceClientMock } from "@selfage/node_service_client/client_mock";
import { assertThat, eq } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

TEST_RUNNER.run({
  name: "ProcessDailyMeterReadingHandlerTest",
  cases: [
    {
      name: "ProcessOneConsumer",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "t1#2024-10-30#consumer1",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
          {
            key: "d1#2024-10-30#consumer1",
            data: {
              w: {
                "season1#ep1#w": {
                  value: 200,
                },
                "season1#ep1#n": {
                  value: 2002,
                },
                "season1#ep2#w": {
                  value: 2200,
                },
                "season1#ep2#n": {
                  value: 22002,
                },
                "season2#ep2#w": {
                  value: 40,
                },
                "season2#ep2#n": {
                  value: 404,
                },
                "season2#ep3#w": {
                  value: 39000,
                },
                "season2#ep3#n": {
                  value: 390003,
                },
                "season3#ep3#w": {
                  value: 10000,
                },
                "season3#ep3#n": {
                  value: 100001,
                },
              },
            },
          },
        ]);
        let clientMock = new (class extends NodeServiceClientMock {
          public async send(request: any): Promise<any> {
            if (request.descriptor === GET_SEASON_PUBLISHER) {
              switch (request.body.seasonId) {
                case "season1":
                  return {
                    publisherId: "publisher1",
                  } as GetSeasonPublisherResponse;
                case "season2":
                  return {
                    publisherId: "publisher1",
                  } as GetSeasonPublisherResponse;
                case "season3":
                  return {
                    publisherId: "publisher3",
                  } as GetSeasonPublisherResponse;
                default:
                  throw new Error("Unexpected");
              }
            } else if (request.descriptor === GET_SEASON_GRADE) {
              switch (request.body.seasonId) {
                case "season1":
                  return {
                    grade: 10,
                  } as GetSeasonGradeResponse;
                case "season2":
                  return {
                    grade: 5,
                  } as GetSeasonGradeResponse;
                case "season3":
                  return {
                    grade: 30,
                  } as GetSeasonGradeResponse;
                default:
                  throw new Error("Unexpected");
              }
            } else {
              throw new Error("Unexpected");
            }
          }
        })();
        let handler = new ProcessDailyMeterReadingHandler(BIGTABLE, clientMock);

        // Execute
        await handler.handle("", {
          rowKey: "t1#2024-10-30#consumer1",
        });

        // Verify
        assertThat(
          (await BIGTABLE.row("f1#consumer1#2024-10-30").get())[0].data,
          eqData({
            w: {
              season1: {
                value: 4,
              },
              season2: {
                value: 40,
              },
              season3: {
                value: 10,
              },
            },
            a: {
              season1: {
                value: 24,
              },
              season2: {
                value: 196,
              },
              season3: {
                value: 300,
              },
            },
            t: {
              ws: {
                value: 520,
              },
            },
          }),
          "final consumer1",
        );
        assertThat(
          (await BIGTABLE.row("d2#2024-10#consumer1#30").get())[0].data,
          eqData({
            t: {
              ws: {
                value: 520,
              },
            },
          }),
          "consumer1 month day",
        );
        assertThat(
          (await BIGTABLE.row("t2#2024-10#consumer1").exists())[0],
          eq(true),
          "consumer1 month task added",
        );
        assertThat(
          (await BIGTABLE.row("d3#2024-10-30#publisher1#consumer1").get())[0]
            .data,
          eqData({
            w: {
              season1: {
                value: 4,
              },
              season2: {
                value: 40,
              },
            },
            a: {
              season1: {
                value: 24,
              },
              season2: {
                value: 196,
              },
            },
            t: {
              nk: {
                value: 406,
              },
            },
          }),
          "publisher1",
        );
        assertThat(
          (await BIGTABLE.row("t3#2024-10-30#publisher1").exists())[0],
          eq(true),
          "publisher1 month task added",
        );
        assertThat(
          (await BIGTABLE.row("d3#2024-10-30#publisher3#consumer1").get())[0]
            .data,
          eqData({
            w: {
              season3: {
                value: 10,
              },
            },
            a: {
              season3: {
                value: 300,
              },
            },
            t: {
              nk: {
                value: 98,
              },
            },
          }),
          "publisher3",
        );
        assertThat(
          (await BIGTABLE.row("t3#2024-10-30#publisher3").exists())[0],
          eq(true),
          "publisher3 month task added",
        );
        assertThat(
          (await BIGTABLE.row("t1#2024-10-30#consumer1").exists())[0],
          eq(false),
          "consumer1 task deleted",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("t");
        await BIGTABLE.deleteRows("d");
        await BIGTABLE.deleteRows("f");
      },
    },
    {
      name: "ProcessOneConsumerWithoutNetwork",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "t1#2024-10-30#consumer1",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
          {
            key: "d1#2024-10-30#consumer1",
            data: {
              w: {
                "season1#ep1#w": {
                  value: 200,
                },
                "season1#ep2#w": {
                  value: 2200,
                },
              },
            },
          },
        ]);
        let clientMock = new (class extends NodeServiceClientMock {
          public async send(request: any): Promise<any> {
            if (request.descriptor === GET_SEASON_PUBLISHER) {
              switch (request.body.seasonId) {
                case "season1":
                  return {
                    publisherId: "publisher1",
                  } as GetSeasonPublisherResponse;
                default:
                  throw new Error("Unexpected");
              }
            } else if (request.descriptor === GET_SEASON_GRADE) {
              switch (request.body.seasonId) {
                case "season1":
                  return {
                    grade: 10,
                  } as GetSeasonGradeResponse;
                default:
                  throw new Error("Unexpected");
              }
            } else {
              throw new Error("Unexpected");
            }
          }
        })();
        let handler = new ProcessDailyMeterReadingHandler(BIGTABLE, clientMock);

        // Execute
        await handler.handle("", {
          rowKey: "t1#2024-10-30#consumer1",
        });

        // Verify
        assertThat(
          (await BIGTABLE.row("f1#consumer1#2024-10-30").get())[0].data,
          eqData({
            w: {
              season1: {
                value: 4,
              },
            },
            a: {
              season1: {
                value: 24,
              },
            },
            t: {
              ws: {
                value: 24,
              },
            },
          }),
          "final consumer1",
        );
        assertThat(
          (await BIGTABLE.row("d2#2024-10#consumer1#30").get())[0].data,
          eqData({
            t: {
              ws: {
                value: 24,
              },
            },
          }),
          "consumer1 month day",
        );
        assertThat(
          (await BIGTABLE.row("t2#2024-10#consumer1").exists())[0],
          eq(true),
          "consumer1 month task added",
        );
        assertThat(
          (await BIGTABLE.row("d3#2024-10-30#publisher1#consumer1").get())[0]
            .data,
          eqData({
            w: {
              season1: {
                value: 4,
              },
            },
            a: {
              season1: {
                value: 24,
              },
            },
          }),
          "publisher1",
        );
        assertThat(
          (await BIGTABLE.row("t3#2024-10-30#publisher1").exists())[0],
          eq(true),
          "publisher1 month task added",
        );
        assertThat(
          (await BIGTABLE.row("t1#2024-10-30#consumer1").exists())[0],
          eq(false),
          "consumer1 task deleted",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("t");
        await BIGTABLE.deleteRows("d");
        await BIGTABLE.deleteRows("f");
      },
    },
    {
      name: "ProcessOneConsumerWithoutWatchTime",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "t1#2024-10-30#consumer1",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
          {
            key: "d1#2024-10-30#consumer1",
            data: {
              w: {
                "season1#ep1#n": {
                  value: 20002,
                },
                "season1#ep2#n": {
                  value: 220002,
                },
              },
            },
          },
        ]);
        let clientMock = new (class extends NodeServiceClientMock {
          public async send(request: any): Promise<any> {
            if (request.descriptor === GET_SEASON_PUBLISHER) {
              switch (request.body.seasonId) {
                case "season1":
                  return {
                    publisherId: "publisher1",
                  } as GetSeasonPublisherResponse;
                default:
                  throw new Error("Unexpected");
              }
            } else if (request.descriptor === GET_SEASON_GRADE) {
              switch (request.body.seasonId) {
                case "season1":
                  return {
                    grade: 10,
                  } as GetSeasonGradeResponse;
                default:
                  throw new Error("Unexpected");
              }
            } else {
              throw new Error("Unexpected");
            }
          }
        })();
        let handler = new ProcessDailyMeterReadingHandler(BIGTABLE, clientMock);

        // Execute
        await handler.handle("", {
          rowKey: "t1#2024-10-30#consumer1",
        });

        // Verify
        assertThat(
          (await BIGTABLE.row("f1#consumer1#2024-10-30").exists())[0],
          eq(false),
          "no final consumer1",
        );
        assertThat(
          (await BIGTABLE.row("d2#2024-10#consumer1#30").exists())[0],
          eq(false),
          "no consumer1 month day",
        );
        assertThat(
          (await BIGTABLE.row("t2#2024-10#consumer1").exists())[0],
          eq(false),
          "no consumer1 month task added",
        );
        assertThat(
          (await BIGTABLE.row("d3#2024-10-30#publisher1#consumer1").get())[0]
            .data,
          eqData({
            t: {
              nk: {
                value: 235,
              },
            },
          }),
          "publisher1",
        );
        assertThat(
          (await BIGTABLE.row("t3#2024-10-30#publisher1").exists())[0],
          eq(true),
          "publisher1 month task added",
        );
        assertThat(
          (await BIGTABLE.row("t1#2024-10-30#consumer1").exists())[0],
          eq(false),
          "consumer1 task deleted",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("t");
        await BIGTABLE.deleteRows("d");
        await BIGTABLE.deleteRows("f");
      },
    },
    {
      name: "RowNotFound",
      execute: async () => {
        // Prepare
        let handler = new ProcessDailyMeterReadingHandler(BIGTABLE, undefined);

        // Execute
        await handler.handle("", {
          rowKey: "t1#2024-10-30#consumer1",
        });

        // Verify no error
      },
    },
    {
      name: "SeasonNotFoundAndIgnored",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "t1#2024-10-30#consumer1",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
          {
            key: "d1#2024-10-30#consumer1",
            data: {
              w: {
                "season1#ep1#w": {
                  value: 2000,
                },
                "season1#ep1#n": {
                  value: 20002,
                },
                "season2#ep2#w": {
                  value: 39000,
                },
                "season2#ep2#n": {
                  value: 390003,
                },
              },
            },
          },
        ]);
        let clientMock = new (class extends NodeServiceClientMock {
          public async send(request: any): Promise<any> {
            if (request.descriptor === GET_SEASON_PUBLISHER) {
              switch (request.body.seasonId) {
                case "season1":
                  return {
                    publisherId: "publisher1",
                  } as GetSeasonPublisherResponse;
                case "season2":
                  throw newNotFoundError("fake error");
                default:
                  throw new Error("Unexpected");
              }
            } else if (request.descriptor === GET_SEASON_GRADE) {
              switch (request.body.seasonId) {
                case "season1":
                  return {
                    grade: 10,
                  } as GetSeasonGradeResponse;
                case "season2":
                  throw newNotFoundError("fake error");
                default:
                  throw new Error("Unexpected");
              }
            } else {
              throw new Error("Unexpected");
            }
          }
        })();
        let handler = new ProcessDailyMeterReadingHandler(BIGTABLE, clientMock);

        // Execute
        await handler.handle("", {
          rowKey: "t1#2024-10-30#consumer1",
        });

        // Verify
        assertThat(
          (await BIGTABLE.row("f1#consumer1#2024-10-30").get())[0].data,
          eqData({
            w: {
              season1: {
                value: 2,
              },
            },
            a: {
              season1: {
                value: 20,
              },
            },
            t: {
              ws: {
                value: 20,
              },
            },
          }),
          "final consumer1",
        );
        assertThat(
          (await BIGTABLE.row("d2#2024-10#consumer1#30").get())[0].data,
          eqData({
            t: {
              ws: {
                value: 20,
              },
            },
          }),
          "consumer1 month day",
        );
        assertThat(
          (await BIGTABLE.row("t2#2024-10#consumer1").exists())[0],
          eq(true),
          "consumer1 month task added",
        );
        assertThat(
          (await BIGTABLE.row("d3#2024-10-30#publisher1#consumer1").get())[0]
            .data,
          eqData({
            w: {
              season1: {
                value: 2,
              },
            },
            a: {
              season1: {
                value: 20,
              },
            },
            t: {
              nk: {
                value: 20,
              },
            },
          }),
          "publisher1",
        );
        assertThat(
          (await BIGTABLE.row("t3#2024-10-30#publisher1").exists())[0],
          eq(true),
          "publisher1 month task added",
        );
        assertThat(
          (await BIGTABLE.row("t1#2024-10-30#consumer1").exists())[0],
          eq(false),
          "consumer1 task deleted",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("t");
        await BIGTABLE.deleteRows("d");
        await BIGTABLE.deleteRows("f");
      },
    },
    {
      name: "WithoutDataRow",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "t1#2024-10-30#consumer1",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
        ]);
        let handler = new ProcessDailyMeterReadingHandler(BIGTABLE, undefined);

        // Execute
        await handler.handle("", {
          rowKey: "t1#2024-10-30#consumer1",
        });

        // Verify
        assertThat(
          (await BIGTABLE.row("t1#2024-10-30#consumer1").exists())[0],
          eq(false),
          "consumer1 task deleted",
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
