// M0 hello-world: pull /v3/reference/tickers/AAPL and print one field.
// Verifies REST plumbing (libcurl + Bearer auth + JSON parse) end-to-end.

#include <cstdlib>
#include <string>

#include <curl/curl.h>
#include <fmt/core.h>
#include <nlohmann/json.hpp>

namespace {

size_t write_cb(void* contents, size_t size, size_t nmemb, void* userp) {
    auto* buf = static_cast<std::string*>(userp);
    buf->append(static_cast<char*>(contents), size * nmemb);
    return size * nmemb;
}

const char* env_or_die(const char* name) {
    const char* v = std::getenv(name);
    if (!v || !*v) {
        fmt::print(stderr, "missing env var: {} (source .env first)\n", name);
        std::exit(2);
    }
    return v;
}

}  // namespace

int main() {
    const char* api_key = env_or_die("MASSIVE_API_KEY");
    const char* base = std::getenv("MASSIVE_REST_BASE");
    if (!base || !*base) base = "https://api.polygon.io";

    const std::string url = fmt::format("{}/v3/reference/tickers/AAPL", base);

    curl_global_init(CURL_GLOBAL_DEFAULT);
    CURL* curl = curl_easy_init();
    if (!curl) {
        fmt::print(stderr, "curl_easy_init failed\n");
        return 1;
    }

    std::string body;
    const std::string auth_hdr = fmt::format("Authorization: Bearer {}", api_key);
    curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, auth_hdr.c_str());
    headers = curl_slist_append(headers, "Accept: application/json");
    headers = curl_slist_append(headers, "Accept-Encoding: gzip");

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &body);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "momentum-hold/0.0");
    curl_easy_setopt(curl, CURLOPT_ACCEPT_ENCODING, "");  // libcurl decompresses

    CURLcode rc = curl_easy_perform(curl);
    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    curl_global_cleanup();

    if (rc != CURLE_OK) {
        fmt::print(stderr, "curl failed: {}\n", curl_easy_strerror(rc));
        return 1;
    }
    if (http_code != 200) {
        fmt::print(stderr, "HTTP {}: {}\n", http_code, body.substr(0, 200));
        return 1;
    }

    auto j = nlohmann::json::parse(body, nullptr, false);
    if (j.is_discarded()) {
        fmt::print(stderr, "JSON parse failed; body[0..200]={}\n", body.substr(0, 200));
        return 1;
    }

    const auto& r = j.at("results");
    fmt::print("ok: ticker={} name={} primary_exchange={} type={} active={}\n",
               r.value("ticker", "?"),
               r.value("name", "?"),
               r.value("primary_exchange", "?"),
               r.value("type", "?"),
               r.value("active", false));
    return 0;
}
