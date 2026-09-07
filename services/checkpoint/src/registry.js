const MANIFEST_ACCEPT = [
  "application/vnd.oci.image.manifest.v1+json",
  "application/vnd.docker.distribution.manifest.v2+json",
  "application/vnd.docker.distribution.manifest.list.v2+json",
].join(", ");

export class RegistryClient {
  constructor({ apiUrl, username, password, fetchImpl = fetch }) {
    this.apiUrl = apiUrl?.replace(/\/$/, "");
    this.authorization = username || password
      ? `Basic ${Buffer.from(`${username}:${password}`, "utf8").toString("base64")}`
      : undefined;
    this.fetch = fetchImpl;
  }

  get enabled() {
    return Boolean(this.apiUrl);
  }

  async deleteImage(imageRef) {
    if (!this.enabled) return false;
    const { repository, reference } = parseRegistryImage(this.apiUrl, imageRef);
    const manifestUrl = `${this.apiUrl}/v2/${repository}/manifests/${encodeURIComponent(reference)}`;
    const head = await this.fetch(manifestUrl, {
      method: "HEAD",
      headers: this.headers(),
    });
    if (head.status === 404) return true;
    if (!head.ok) throw new Error(`Registry manifest lookup failed with ${head.status}`);
    const digest = head.headers.get("docker-content-digest");
    if (!digest) throw new Error("Registry did not return Docker-Content-Digest");
    const result = await this.fetch(
      `${this.apiUrl}/v2/${repository}/manifests/${encodeURIComponent(digest)}`,
      { method: "DELETE", headers: this.headers() }
    );
    if (!result.ok && result.status !== 404) {
      throw new Error(`Registry manifest delete failed with ${result.status}`);
    }
    return true;
  }

  headers() {
    return {
      Accept: MANIFEST_ACCEPT,
      ...(this.authorization ? { Authorization: this.authorization } : {}),
    };
  }
}

export function parseRegistryImage(apiUrl, imageRef) {
  const registryHost = new URL(apiUrl).host;
  const withoutDigest = imageRef.split("@")[0];
  const slash = withoutDigest.indexOf("/");
  const imageHost = slash === -1 ? "" : withoutDigest.slice(0, slash);
  let repositoryAndTag = imageHost === registryHost ? withoutDigest.slice(slash + 1) : withoutDigest;
  const lastSlash = repositoryAndTag.lastIndexOf("/");
  const lastColon = repositoryAndTag.lastIndexOf(":");
  const reference = lastColon > lastSlash ? repositoryAndTag.slice(lastColon + 1) : "latest";
  if (lastColon > lastSlash) repositoryAndTag = repositoryAndTag.slice(0, lastColon);
  return { repository: repositoryAndTag, reference };
}
