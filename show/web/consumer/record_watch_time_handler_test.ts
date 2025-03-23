import "../../../local/env";
import { BIGTABLE } from "../../../common/bigtable";
import { eqData } from "../../../common/bigtable_data_matcher";
import { CachedSessionFetcher } from "./common/cached_session_fetcher";
import { RecordWatchTimeHandler } from "./record_watch_time_handler";
import { FetchSessionAndCheckCapabilityResponse } from "@phading/user_session_service_interface/node/interface";
import { NodeServiceClientMock } from "@selfage/node_service_client/client_mock";
import { assertThat, eq } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

TEST_RUNNER.run({
  name: "RecordWatchTimeHandlerTest",
  cases: [
    {
      name: "IncrementTwice",
      execute: async () => {
        // Prepare
        let clientMock = new NodeServiceClientMock();
        clientMock.response = {
          accountId: "consumer1",
          capabilities: {
            canConsume: true,
          },
        } as FetchSessionAndCheckCapabilityResponse;
        // 2024-10-26 23:xx:xx
        let handler = new RecordWatchTimeHandler(
          BIGTABLE,
          new CachedSessionFetcher(clientMock),
          () => new Date(1729983732156),
        );

        // Execute
        await handler.handle(
          "",
          {
            seasonId: "season1",
            episodeId: "ep1",
            watchTimeMs: 125,
          },
          "session1",
        );

        // Verify
        assertThat(
          (await BIGTABLE.row(`t1#2024-10-26#consumer1`).exists())[0],
          eq(true),
          "task added",
        );
        assertThat(
          (await BIGTABLE.row(`d1#2024-10-26#consumer1`).get())[0].data,
          eqData({
            w: {
              "season1#ep1#w": {
                value: 125,
              },
            },
          }),
          "1st count",
        );

        // Execute
        await handler.handle(
          "",
          {
            seasonId: "season1",
            episodeId: "ep1",
            watchTimeMs: 200,
          },
          "session1",
        );

        // Verify
        assertThat(
          (await BIGTABLE.row(`d1#2024-10-26#consumer1`).get())[0].data,
          eqData({
            w: {
              "season1#ep1#w": {
                value: 325,
              },
            },
          }),
          "2nd count",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("d");
        await BIGTABLE.deleteRows("t");
      },
    },
    {
      name: "GetDateBasedOnTimezoneOffset",
      execute: async () => {
        // Prepare
        let clientMock = new NodeServiceClientMock();
        clientMock.response = {
          accountId: "consumer1",
          capabilities: {
            canConsume: true,
          },
        } as FetchSessionAndCheckCapabilityResponse;
        // 2025-01-01 01:xx:xx UTC
        let handler = new RecordWatchTimeHandler(
          BIGTABLE,
          new CachedSessionFetcher(clientMock),
          () => new Date(1735696113000),
        );

        // Execute
        await handler.handle(
          "",
          {
            seasonId: "season1",
            episodeId: "ep1",
            watchTimeMs: 300,
          },
          "session1",
        );

        // Verify
        assertThat(
          (await BIGTABLE.row(`t1#2024-12-31#consumer1`).exists())[0],
          eq(true),
          "task added",
        );
        assertThat(
          (await BIGTABLE.row(`d1#2024-12-31#consumer1`).get())[0].data,
          eqData({
            w: {
              "season1#ep1#w": {
                value: 300,
              },
            },
          }),
          "count",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("d");
        await BIGTABLE.deleteRows("t");
      },
    },
  ],
});
