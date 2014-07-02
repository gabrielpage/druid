package io.druid.storage.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class TestAWSCredentialsProvider {
  @Test
  public void testWithFixedAWSKeys() {
    S3StorageDruidModule module = new S3StorageDruidModule();

    AWSCredentialsConfig config = mock(AWSCredentialsConfig.class);
    doReturn("accessKeySample").when(config).getAccessKey();
    doReturn("secretKeySample").when(config).getSecretKey();

    AWSCredentialsProvider provider = module.getAWSCredentialsProvider(config);
    AWSCredentials credentials = provider.getCredentials();
    assertEquals(credentials.getAWSAccessKeyId(), "accessKeySample");
    assertEquals(credentials.getAWSSecretKey(), "secretKeySample");

    // try to create
    module.getRestS3Service(provider);
  }

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testWithFileSessionCredentials() throws IOException {
    S3StorageDruidModule module = new S3StorageDruidModule();

    AWSCredentialsConfig config = mock(AWSCredentialsConfig.class);
    doReturn("").when(config).getAccessKey();
    doReturn("").when(config).getSecretKey();
    File file = folder.newFile();
    PrintWriter out = new PrintWriter(file.getAbsolutePath());
    out.println("sessionToken=sessionTokenSample\nsecretKey=secretKeySample\naccessKey=accessKeySample");
    out.close();
    doReturn(file.getAbsolutePath()).when(config).getFileSessionCredentials();

    AWSCredentialsProvider provider = module.getAWSCredentialsProvider(config);
    AWSCredentials credentials = provider.getCredentials();
    assertTrue(credentials instanceof AWSSessionCredentials);
    AWSSessionCredentials sessionCredentials = (AWSSessionCredentials) credentials;
    assertEquals(sessionCredentials.getAWSAccessKeyId(), "accessKeySample");
    assertEquals(sessionCredentials.getAWSSecretKey(), "secretKeySample");
    assertEquals(sessionCredentials.getSessionToken(), "sessionTokenSample");

    // try to create
    module.getRestS3Service(provider);
  }
}
