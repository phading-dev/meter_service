import { BIGTABLE } from "../../../common/bigtable";
import { GetMonthlyBatchHandler } from "./get_monthly_batch_handler";
import { GET_MONTHLY_BATCH_RESPONSE } from "@phading/product_meter_service_interface/show/backend/publisher/interface";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertThat } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

TEST_RUNNER.run({
  name: "GetMonthlyBatchHandlerTest",
  cases: [
    {
      name: "UntilThisMonth_GetFirstBatch_GetSecondBatch",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "q5#2024-10#publisher1",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
          {
            key: "q5#2024-10#publisher2",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
          {
            key: "q5#2024-10#publisher3",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
          {
            key: "q5#2024-11#publisher4",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
          {
            key: "q1#2024-12-02#consumer1",
            data: {
              w: {
                "season1#ep1": {
                  value: 10,
                },
              },
            },
          },
          {
            key: "q3#2024-12-02#consumer1",
            data: {
              c: {
                r: {
                  value: "",
                },
              },
            },
          },
        ]);
        // 2024-11-02 04:xx:xx UTC
        let handler = new GetMonthlyBatchHandler(
          2,
          BIGTABLE,
          () => new Date(1730522913000),
        );

        // Execute
        let response = await handler.handle("", {});

        // Verify
        assertThat(
          response,
          eqMessage(
            {
              rowKeys: ["q5#2024-10#publisher1", "q5#2024-10#publisher2"],
              cursor: "q5#2024-10#publisher2",
            },
            GET_MONTHLY_BATCH_RESPONSE,
          ),
          "response 1",
        );

        // Execute
        response = await handler.handle("", {
          cursor: "q5#2024-10#publisher2",
        });

        // Verify
        assertThat(
          response,
          eqMessage(
            {
              rowKeys: ["q5#2024-10#publisher3"],
            },
            GET_MONTHLY_BATCH_RESPONSE,
          ),
          "response 2",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("q");
      },
    },
    {
      name: "GetUntilUnprocessedQ1Rows",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "q5#2024-10#publisher1",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
          {
            key: "q5#2024-11#publisher2",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
          {
            key: "q1#2024-11-02#consumer1",
            data: {
              w: {
                "season1#ep1": {
                  value: 10,
                },
              },
            },
          },
          {
            key: "q3#2024-12-02#consumer1",
            data: {
              c: {
                r: {
                  value: "",
                },
              },
            },
          },
        ]);
        // 2024-12-02 04:xx:xx UTC
        let handler = new GetMonthlyBatchHandler(
          2,
          BIGTABLE,
          () => new Date(1733114913000),
        );

        // Execute
        let response = await handler.handle("", {});

        // Verify
        assertThat(
          response,
          eqMessage(
            {
              rowKeys: ["q5#2024-10#publisher1"],
            },
            GET_MONTHLY_BATCH_RESPONSE,
          ),
          "response",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("q");
      },
    },
    {
      name: "GetUntilUnprocessedQ3Rows",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "q5#2024-10#publisher1",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
          {
            key: "q5#2024-11#publisher2",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
          {
            key: "q1#2024-12-02#consumer1",
            data: {
              w: {
                "season1#ep1": {
                  value: 10,
                },
              },
            },
          },
          {
            key: "q3#2024-11-02#consumer1",
            data: {
              c: {
                r: {
                  value: "",
                },
              },
            },
          },
        ]);
        // 2024-12-02 04:xx:xx UTC
        let handler = new GetMonthlyBatchHandler(
          2,
          BIGTABLE,
          () => new Date(1733114913000),
        );

        // Execute
        let response = await handler.handle("", {});

        // Verify
        assertThat(
          response,
          eqMessage(
            {
              rowKeys: ["q5#2024-10#publisher1"],
            },
            GET_MONTHLY_BATCH_RESPONSE,
          ),
          "response",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("q");
      },
    },
  ],
});
