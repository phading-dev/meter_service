import { BIGTABLE } from "../../../common/bigtable";
import { eqData } from "../../../common/bigtable_data_matcher";
import { ProcessDailyMeterReadingHandler } from "./process_daily_meter_reading_handler";
import {
  GET_SEASON_PUBLISHER_AND_GRADE,
  GET_VIDEO_DURATION_AND_SIZE,
  GetSeasonPublisherAndGradeResponse,
  GetVideoDurationAndSizeResponse,
} from "@phading/product_service_interface/show/backend/interface";
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
            key: "q1#2024-10-30#consumer1",
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
                "season1#ep1": {
                  value: 200,
                },
                "season1#ep2": {
                  value: 2200,
                },
                "season2#ep2": {
                  value: 40,
                },
                "season2#ep3": {
                  value: 39000,
                },
                "season3#ep3": {
                  value: 10000,
                },
              },
            },
          },
        ]);
        let clientMock = new (class extends NodeServiceClientMock {
          public async send(request: any): Promise<any> {
            if (request.descriptor === GET_SEASON_PUBLISHER_AND_GRADE) {
              switch (request.body.seasonId) {
                case "season1":
                  return {
                    seasonId: "season1",
                    grade: 10,
                    publisherId: "publisher1",
                  } as GetSeasonPublisherAndGradeResponse;
                case "season2":
                  return {
                    seasonId: "season2",
                    grade: 5,
                    publisherId: "publisher1",
                  } as GetSeasonPublisherAndGradeResponse;
                case "season3":
                  return {
                    seasonId: "season3",
                    grade: 30,
                    publisherId: "publisher3",
                  } as GetSeasonPublisherAndGradeResponse;
                default:
                  throw new Error("Unexpected");
              }
            } else if (request.descriptor === GET_VIDEO_DURATION_AND_SIZE) {
              if (
                request.body.seasonId === "season1" &&
                request.body.episodeId === "ep1"
              ) {
                return {
                  videoSize: 360000,
                  videoDurationSec: 60,
                } as GetVideoDurationAndSizeResponse;
              } else if (
                request.body.seasonId === "season1" &&
                request.body.episodeId === "ep2"
              ) {
                return {
                  videoSize: 1020000,
                  videoDurationSec: 500,
                } as GetVideoDurationAndSizeResponse;
              } else if (
                request.body.seasonId === "season2" &&
                request.body.episodeId === "ep2"
              ) {
                return {
                  videoSize: 95000,
                  videoDurationSec: 200,
                } as GetVideoDurationAndSizeResponse;
              } else if (
                request.body.seasonId === "season2" &&
                request.body.episodeId === "ep3"
              ) {
                return {
                  videoSize: 320000,
                  videoDurationSec: 700,
                } as GetVideoDurationAndSizeResponse;
              } else if (
                request.body.seasonId === "season3" &&
                request.body.episodeId === "ep3"
              ) {
                return {
                  videoSize: 1000000,
                  videoDurationSec: 100,
                } as GetVideoDurationAndSizeResponse;
              } else {
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
          rowKey: "q1#2024-10-30#consumer1",
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
              w: {
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
              w: {
                value: 520,
              },
            },
          }),
          "consumer1 month day",
        );
        assertThat(
          (await BIGTABLE.row("q2#2024-10#consumer1").exists())[0],
          eq(true),
          "consumer1 month enqueued",
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
              kb: {
                value: 26,
              },
            },
          }),
          "publisher1",
        );
        assertThat(
          (await BIGTABLE.row("q3#2024-10-30#publisher1").exists())[0],
          eq(true),
          "publisher1 month enqueued",
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
              kb: {
                value: 98,
              },
            },
          }),
          "publisher3",
        );
        assertThat(
          (await BIGTABLE.row("q3#2024-10-30#publisher3").exists())[0],
          eq(true),
          "publisher3 month enqueued",
        );
        assertThat(
          (await BIGTABLE.row("q1#2024-10-30#consumer1").exists())[0],
          eq(false),
          "consumer1 queue deleted",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("q");
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
          rowKey: "q1#2024-10-30#consumer1",
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
            key: "q1#2024-10-30#consumer1",
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
                "season1#ep1": {
                  value: 2000,
                },
                "season2#ep2": {
                  value: 39000,
                },
              },
            },
          },
        ]);
        let clientMock = new (class extends NodeServiceClientMock {
          public async send(request: any): Promise<any> {
            if (request.descriptor === GET_SEASON_PUBLISHER_AND_GRADE) {
              switch (request.body.seasonId) {
                case "season1":
                  return {
                    seasonId: "season1",
                    grade: 10,
                    publisherId: "publisher1",
                  } as GetSeasonPublisherAndGradeResponse;
                case "season2":
                  throw newNotFoundError("fake error");
                default:
                  throw new Error("Unexpected");
              }
            } else if (request.descriptor === GET_VIDEO_DURATION_AND_SIZE) {
              if (
                request.body.seasonId === "season1" &&
                request.body.episodeId === "ep1"
              ) {
                return {
                  videoSize: 360000,
                  videoDurationSec: 60,
                } as GetVideoDurationAndSizeResponse;
              } else if (
                request.body.seasonId === "season2" &&
                request.body.episodeId === "ep2"
              ) {
                return {
                  videoSize: 95000,
                  videoDurationSec: 200,
                } as GetVideoDurationAndSizeResponse;
              } else {
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
          rowKey: "q1#2024-10-30#consumer1",
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
              w: {
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
              w: {
                value: 20,
              },
            },
          }),
          "consumer1 month day",
        );
        assertThat(
          (await BIGTABLE.row("q2#2024-10#consumer1").exists())[0],
          eq(true),
          "consumer1 month enqueued",
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
              kb: {
                value: 12,
              },
            },
          }),
          "publisher1",
        );
        assertThat(
          (await BIGTABLE.row("q3#2024-10-30#publisher1").exists())[0],
          eq(true),
          "publisher1 month enqueued",
        );
        assertThat(
          (await BIGTABLE.row("q1#2024-10-30#consumer1").exists())[0],
          eq(false),
          "consumer1 queue deleted",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("q");
        await BIGTABLE.deleteRows("d");
        await BIGTABLE.deleteRows("f");
      },
    },
    {
      name: "EpisodeNotFoundAndIgnored",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "q1#2024-10-30#consumer1",
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
                "season1#ep1": {
                  value: 2000,
                },
                "season2#ep2": {
                  value: 39000,
                },
              },
            },
          },
        ]);
        let clientMock = new (class extends NodeServiceClientMock {
          public async send(request: any): Promise<any> {
            if (request.descriptor === GET_SEASON_PUBLISHER_AND_GRADE) {
              switch (request.body.seasonId) {
                case "season1":
                  return {
                    seasonId: "season1",
                    grade: 10,
                    publisherId: "publisher1",
                  } as GetSeasonPublisherAndGradeResponse;
                case "season2":
                  return {
                    seasonId: "season2",
                    grade: 5,
                    publisherId: "publisher1",
                  } as GetSeasonPublisherAndGradeResponse;
                default:
                  throw new Error("Unexpected");
              }
            } else if (request.descriptor === GET_VIDEO_DURATION_AND_SIZE) {
              if (
                request.body.seasonId === "season1" &&
                request.body.episodeId === "ep1"
              ) {
                return {
                  videoSize: 360000,
                  videoDurationSec: 60,
                } as GetVideoDurationAndSizeResponse;
              } else if (
                request.body.seasonId === "season2" &&
                request.body.episodeId === "ep2"
              ) {
                throw newNotFoundError("fake error");
              } else {
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
          rowKey: "q1#2024-10-30#consumer1",
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
              w: {
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
              w: {
                value: 20,
              },
            },
          }),
          "consumer1 month day",
        );
        assertThat(
          (await BIGTABLE.row("q2#2024-10#consumer1").exists())[0],
          eq(true),
          "consumer1 month enqueued",
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
              kb: {
                value: 12,
              },
            },
          }),
          "publisher1",
        );
        assertThat(
          (await BIGTABLE.row("q3#2024-10-30#publisher1").exists())[0],
          eq(true),
          "publisher1 month enqueued",
        );
        assertThat(
          (await BIGTABLE.row("q1#2024-10-30#consumer1").exists())[0],
          eq(false),
          "consumer1 queue deleted",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("q");
        await BIGTABLE.deleteRows("d");
        await BIGTABLE.deleteRows("f");
      },
    },
  ],
});
