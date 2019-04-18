const AWS = require('aws-sdk')
const zlib = require('zlib')

class S3ParallelFetcher {
    constructor(conf) {
        this.s3 = new AWS.S3(conf)
    }

    async list(Bucket, Prefix) {
        const listBucketParam = {
            Bucket,
            Prefix,
            MaxKeys: 1000
        };
        const keyData = []

        for(;;) {
            const data = await this.s3.listObjectsV2(listBucketParam).promise()
            data.Contents.forEach(v => {
                keyData.push({
                    Bucket,
                    Key: v.Key
                })
            })
            if (!data.IsTruncated) return keyData
            listBucketParam.ContinuationToken = data.NextContinuationToken;
        }
    }

    async fetchall(Bucket, Prefix) {
        const listKeys = await this.list(Bucket, Prefix)
        const jobs = []
        const results = []
        for(const d of listKeys) {
            const job = this.s3.getObject(d).promise().then((data) => {
                return new Promise((resolve, reject) => {
                    zlib.gunzip(data.Body, (err, buf) => {
                        if (err) reject(err)
                        for(const row of buf.toString().split('\n')) {
                            if (row) results.push(JSON.parse(row))
                        }
                        resolve()
                    })
                })
            })
            jobs.push(job)
        }
        await Promise.all(jobs)
        return results
    }
}

module.exports = S3ParallelFetcher;