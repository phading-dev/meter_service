import {
  CACHE_SIZE_OF_SESSION,
  CACHE_TTL_MS_OF_SESSION,
} from "../../../../common/constants";
import { SERVICE_CLIENT } from "../../../../common/service_client";
import { newFetchSessionAndCheckCapabilityRequest } from "@phading/user_session_service_interface/node/client";
import { newUnauthorizedError } from "@selfage/http_error";
import { NodeServiceClient } from "@selfage/node_service_client";
import { LRUCache } from "lru-cache";

export class CachedSessionFetcher {
  public static create(): CachedSessionFetcher {
    return new CachedSessionFetcher(SERVICE_CLIENT);
  }

  public lruCache: LRUCache<string, string>;

  public constructor(private serviceClient: NodeServiceClient) {
    this.lruCache = new LRUCache({
      max: CACHE_SIZE_OF_SESSION,
      ttl: CACHE_TTL_MS_OF_SESSION,
    });
  }

  public async getAccountId(
    sessionStr: string,
    purpose: string,
  ): Promise<string> {
    let accountIdCached = this.lruCache.get(sessionStr);
    if (!accountIdCached) {
      let { accountId, capabilities } = await this.serviceClient.send(
        newFetchSessionAndCheckCapabilityRequest({
          signedSession: sessionStr,
          capabilitiesMask: {
            checkCanConsume: true,
          },
        }),
      );
      if (!capabilities.canConsume) {
        throw newUnauthorizedError(
          `Account ${accountId} not allowed to ${purpose}.`,
        );
      }
      this.lruCache.set(sessionStr, accountId);
      accountIdCached = accountId;
    }
    return accountIdCached;
  }
}

export let CACHED_SESSION_FETCHER = CachedSessionFetcher.create();
