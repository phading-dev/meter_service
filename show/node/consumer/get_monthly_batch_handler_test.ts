import "../../../local/env";
import { BIGTABLE } from "../../../common/bigtable";
import { GetMonthlyBatchHandler } from "./get_monthly_batch_handler";
import { GET_MONTHLY_BATCH_RESPONSE } from "@phading/meter_service_interface/show/node/consumer/interface";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertThat } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

TEST_RUNNER.run({
  name: "GetMonthlyBatchHandlerTest",
  cases: [
    {
      name: "GetFirstBatch_GetSecondBatch",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "t2#2024-10#consumer1",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
          {
            key: "t2#2024-10#consumer2",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
          {
            key: "t2#2024-10#consumer3",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
          {
            key: "t1#2024-12-10#consumer1",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
        ]);
        // Now is 2024-11-01 10:xx:xx UTC
        let handler = new GetMonthlyBatchHandler(
          2,
          BIGTABLE,
          () => new Date(1730455658000),
        );

        // Execute
        let response = await handler.handle("", {});

        // Verify
        assertThat(
          response,
          eqMessage(
            {
              rowKeys: ["t2#2024-10#consumer1", "t2#2024-10#consumer2"],
              cursor: "t2#2024-10#consumer2",
            },
            GET_MONTHLY_BATCH_RESPONSE,
          ),
          "response 1st",
        );

        // Execute
        response = await handler.handle("", {
          cursor: "t2#2024-10#consumer2",
        });

        // Verify
        assertThat(
          response,
          eqMessage(
            {
              rowKeys: ["t2#2024-10#consumer3"],
            },
            GET_MONTHLY_BATCH_RESPONSE,
          ),
          "response 2nd",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("t");
      },
    },
    {
      name: "EmptyDueToUnprocessedDate",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "t2#2024-10#consumer1",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
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
        // Now is 2024-11-01 10:xx:xx UTC
        let handler = new GetMonthlyBatchHandler(
          2,
          BIGTABLE,
          () => new Date(1730455658000),
        );

        // Execute
        let response = await handler.handle("", {});

        // Verify
        assertThat(
          response,
          eqMessage(
            {
              rowKeys: [],
            },
            GET_MONTHLY_BATCH_RESPONSE,
          ),
          "response",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("t");
      },
    },
  ],
});
