// M0 hello-world: list a prefix and download one minute-agg flat file.
// Verifies S3 plumbing (custom endpoint, path-style, SigV4) against files.massive.com.

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <fmt/core.h>

namespace {

const char* env_or_die(const char* name) {
    const char* v = std::getenv(name);
    if (!v || !*v) {
        fmt::print(stderr, "missing env var: {} (source .env first)\n", name);
        std::exit(2);
    }
    return v;
}

const char* env_or(const char* name, const char* fallback) {
    const char* v = std::getenv(name);
    return (v && *v) ? v : fallback;
}

}  // namespace

int main() {
    const char* access_key = env_or_die("MASSIVE_S3_ACCESS_KEY_ID");
    const char* secret_key = env_or_die("MASSIVE_S3_SECRET_ACCESS_KEY");
    const char* endpoint = env_or("MASSIVE_S3_ENDPOINT", "https://files.massive.com");
    const char* bucket = env_or("MASSIVE_S3_BUCKET", "flatfiles");
    const char* region = env_or("MASSIVE_S3_REGION", "us-east-1");

    // Pick a flat file we're confident is published. 2024-06-04 = Tuesday.
    const std::string key = "us_stocks_sip/minute_aggs_v1/2024/06/2024-06-04.csv.gz";
    const std::string prefix = "us_stocks_sip/minute_aggs_v1/2024/06/";
    const std::filesystem::path out_path = "data/_staging/hello_s3_2024-06-04.csv.gz";

    Aws::SDKOptions options;
    Aws::InitAPI(options);
    int rc = 0;
    {
        Aws::Client::ClientConfiguration cfg;
        cfg.endpointOverride = endpoint;
        cfg.scheme = Aws::Http::Scheme::HTTPS;
        cfg.region = region;
        cfg.verifySSL = true;

        Aws::Auth::AWSCredentials creds(access_key, secret_key);

        Aws::S3::S3Client s3(
            creds, cfg,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            /* useVirtualAddressing */ false);

        // 1. ListObjectsV2 — proves auth + signing work.
        {
            Aws::S3::Model::ListObjectsV2Request req;
            req.SetBucket(bucket);
            req.SetPrefix(prefix);
            req.SetMaxKeys(5);
            auto out = s3.ListObjectsV2(req);
            if (!out.IsSuccess()) {
                const auto& e = out.GetError();
                fmt::print(stderr, "ListObjectsV2 failed: {} | {}\n",
                           e.GetExceptionName().c_str(),
                           e.GetMessage().c_str());
                rc = 1;
            } else {
                fmt::print("ok: list {} → {} object(s) under {}\n",
                           bucket, out.GetResult().GetContents().size(), prefix);
                for (const auto& obj : out.GetResult().GetContents()) {
                    fmt::print("    {}  ({} bytes)\n",
                               obj.GetKey().c_str(), obj.GetSize());
                }
            }
        }

        // 2. GetObject — proves the bulk download path works.
        if (rc == 0) {
            std::filesystem::create_directories(out_path.parent_path());
            Aws::S3::Model::GetObjectRequest req;
            req.SetBucket(bucket);
            req.SetKey(key);
            auto out = s3.GetObject(req);
            if (!out.IsSuccess()) {
                const auto& e = out.GetError();
                fmt::print(stderr, "GetObject failed: {} | {}\n",
                           e.GetExceptionName().c_str(),
                           e.GetMessage().c_str());
                rc = 1;
            } else {
                std::ofstream f(out_path, std::ios::binary);
                f << out.GetResult().GetBody().rdbuf();
                f.close();
                const auto sz = std::filesystem::file_size(out_path);
                fmt::print("ok: get {} → {} ({} bytes)\n",
                           key, out_path.string(), sz);
            }
        }
    }
    Aws::ShutdownAPI(options);
    return rc;
}
