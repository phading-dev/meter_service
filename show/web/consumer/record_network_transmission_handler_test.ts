import { BIGTABLE } from "../../../common/bigtable";
import { eqData } from "../../../common/bigtable_data_matcher";
import { CachedSessionExchanger } from "./common/cached_session_exchanger";
import { RecordNetworkTransmissionHandler } from "./record_network_transmission_handler";
import { ExchangeSessionAndCheckCapabilityResponse } from "@phading/user_session_service_interface/node/interface";
import { NodeServiceClientMock } from "@selfage/node_service_client/client_mock";
import { assertThat, eq } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

TEST_RUNNER.run({
  name: "RecordNetworkTransmissionHandlerTest",
  cases: [
    {
      name: "IncrementTwice",
      execute: async () => {
        // Prepare
        let clientMock = new NodeServiceClientMock();
        clientMock.response = {
          accountId: "consumer1",
          capabilities: {
            canConsumeShows: true,
          },
        } as ExchangeSessionAndCheckCapabilityResponse;
        // 2024-10-26 23:xx:xx
        let handler = new RecordNetworkTransmissionHandler(
          BIGTABLE,
          new CachedSessionExchanger(clientMock),
          () => new Date(1729983732156),
        );

        // Execute
        await handler.handle(
          "",
          {
            seasonId: "season1",
            episodeId: "ep1",
            transmittedBytes: 1024,
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
              "season1#ep1#n": {
                value: 1024,
              },
            },
          }),
          "1st transmission",
        );

        // Execute
        await handler.handle(
          "",
          {
            seasonId: "season1",
            episodeId: "ep1",
            transmittedBytes: 2048,
          },
          "session1",
        );

        // Verify
        assertThat(
          (await BIGTABLE.row(`d1#2024-10-26#consumer1`).get())[0].data,
          eqData({
            w: {
              "season1#ep1#n": {
                value: 3072,
              },
            },
          }),
          "2nd transmission",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("d");
        await BIGTABLE.deleteRows("t");
      },
    },
  ],
});
