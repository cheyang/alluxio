/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs.oss;

import alluxio.retry.RetryPolicy;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.ObjectMetadata;
import org.apache.commons.httpclient.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A wrapper around an {@link OSSObject} which handles skips efficiently.
 */
@NotThreadSafe
public class OSSAbortableInputStream extends InputStream {
    private static final Logger LOG = LoggerFactory.getLogger(OSSAbortableInputStream.class);

    /** The OSS client for OSS operations. */
    private final OSS mClient;
    /** Bucket name of the Alluxio OSS bucket. */
    private final String mBucketName;
    /** The path of the object to read. */
    private final String mKey;

    /** The underlying input stream. */
    private BufferedInputStream mIn;

    /** The current position of the stream. */
    private long mPos;

    /**
     * Policy determining the retry behavior in case the key does not exist. The key may not exist
     * because of eventual consistency.
     */
    private final RetryPolicy mRetryPolicy;

    /**
     * Constructor for an input stream of an object in OSS using the java-oss-sdk implementation to read
     * the data. The stream will be positioned at the start of the file.
     *
     * @param bucketName the bucket the object resides in
     * @param key the path of the object to read
     * @param client the OSS client to use for operations
     * @param retryPolicy retry policy in case the key does not exist
     */
    public OSSAbortableInputStream(String bucketName, String key, OSS client, RetryPolicy retryPolicy) {
        this(bucketName, key, client, 0L, retryPolicy);
    }

    /**
     * Constructor for an input stream of an object in OSS using the java-oss-sdk implementation to read the
     * data. The stream will be positioned at the specified position.
     *
     * @param bucketName the bucket the object resides in
     * @param key the path of the object to read
     * @param client the OSS client to use for operations
     * @param position the position to begin reading from
     * @param retryPolicy retry policy in case the key does not exist
     */
    public OSSAbortableInputStream(String bucketName, String key, OSS client,
                          long position, RetryPolicy retryPolicy) {
        mBucketName = bucketName;
        mKey = key;
        mClient = client;
        mPos = position;
        mRetryPolicy = retryPolicy;
    }

    @Override
    public void close() {
        closeStream();
    }

    @Override
    public int read() throws IOException {
        if (mIn == null) {
            openStream();
        }
        int value = mIn.read();
        if (value != -1) { // valid data read
            mPos++;
        }
        return value;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int offset, int length) throws IOException {
        if (length == 0) {
            return 0;
        }
        if (mIn == null) {
            openStream();
        }
        int read = mIn.read(b, offset, length);
        if (read != -1) {
            mPos += read;
        }
        return read;
    }

    @Override
    public long skip(long n) throws IOException {
        if (n <= 0) {
            return 0;
        }
        closeStream();
        mPos += n;
        openStream();
        return n;
    }

    /**
     * Opens a new stream at mPos if the wrapped stream mIn is null.
     */
    private void openStream() throws IOException {
        if (mIn != null) { // stream is already open
            return;
        }
        GetObjectRequest getReq = new GetObjectRequest(mBucketName, mKey);
        if (mPos > 0) {
            getReq.setRange(mPos, -1);
        }
        OSSException lastException = null;
        while (mRetryPolicy.attempt()) {
            try {
                OSSObject mOSSObject = mClient.getObject(getReq);
                mIn = new BufferedInputStream(mOSSObject.getObjectContent());
                return ;
            } catch (OSSException e) {
                LOG.warn("Attempt {} to open key {} in bucket {} failed with exception : {}",
                        mRetryPolicy.getAttemptCount(), mKey, mBucketName, e.toString());
                if (!e.getErrorCode().equals("NoSuchKey")) {
                    throw new IOException(e);
                }
                // Key does not exist
                lastException = e;
            }
        }
        // Failed after retrying key does not exist
        throw new IOException(lastException);
    }

    /**
     * Closes the current stream.
     */
    private void closeStream() throws IOException {
        if (mIn == null) {
            return;
        }
        mIn.close();
        mIn = null;
    }
}