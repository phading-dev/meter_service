import { BIGTABLE } from "../../../common/bigtable";
import { eqData } from "../../../common/bigtable_data_matcher";
import { ProcessDailyMeterReadingHandler } from "./process_daily_meter_reading_handler";
import {
  GET_SEASON_PUBLISHER_AND_GRADE,
  GetSeasonPublisherAndGradeResponse,
} from "@phading/product_service_interface/consumer/show/backend/interface";
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
                  value: 3900,
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
            assertThat(
              request.descriptor,
              eq(GET_SEASON_PUBLISHER_AND_GRADE),
              "request descriptor",
            );
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
                value: 5,
              },
              season3: {
                value: 10,
              },
            },
            t: {
              w: {
                value: 345,
              },
            },
          }),
          "final consumer1",
        );
        assertThat(
          (await BIGTABLE.row("t2#2024-10#consumer1#30").get())[0].data,
          eqData({
            t: {
              w: {
                value: 345,
              },
            },
          }),
          "consumer1 month day",
        );
        assertThat(
          (await BIGTABLE.row("t6#2024-10#consumer1").exists())[0],
          eq(true),
          "consumer1 month",
        );
        assertThat(
          (await BIGTABLE.row("t3#2024-10-30#publisher1#consumer1").get())[0]
            .data,
          eqData({
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
                value: 3900,
              },
            },
            a: {
              season1: {
                value: 24,
              },
              season2: {
                value: 21,
              },
            },
          }),
          "publisher1",
        );
        assertThat(
          (await BIGTABLE.row("t3#2024-10-30#publisher3#consumer1").get())[0]
            .data,
          eqData({
            w: {
              "season3#ep3": {
                value: 10000,
              },
            },
            a: {
              season3: {
                value: 300,
              },
            },
          }),
          "publisher3",
        );
        assertThat(
          (await BIGTABLE.row("t4#2024-10-30#publisher1").exists())[0],
          eq(true),
          "publisher1 avaialble",
        );
        assertThat(
          (await BIGTABLE.row("t4#2024-10-30#publisher3").exists())[0],
          eq(true),
          "publisher3 avaialble",
        );
        assertThat(
          (await BIGTABLE.row("t1#2024-10-30#consumer1").exists())[0],
          eq(false),
          "consumer1 original data deleted",
        );
      },
      tearDown: async () => {
        await Promise.all([BIGTABLE.deleteRows("t"), BIGTABLE.deleteRows("f")]);
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
  ],
});
