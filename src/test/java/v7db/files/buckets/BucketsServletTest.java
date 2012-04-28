/**
 * Copyright (c) 2012, Thilo Planz. All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package v7db.files.buckets;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;

import javax.servlet.http.HttpServletResponse;

import jmockmongo.MockMongo;
import jmockmongo.MockMongoTestCaseSupport;

import org.apache.commons.codec.digest.DigestUtils;
import org.bson.BasicBSONObject;
import org.xml.sax.SAXException;

import v7db.files.Main;
import v7db.files.mongodb.MongoContentStorage;
import v7db.files.spi.ContentSHA;

import com.meterware.httpunit.GetMethodWebRequest;
import com.meterware.httpunit.HttpException;
import com.meterware.httpunit.HttpNotFoundException;
import com.meterware.httpunit.PutMethodWebRequest;
import com.meterware.httpunit.WebRequest;
import com.meterware.httpunit.WebResponse;
import com.meterware.servletunit.ServletRunner;
import com.meterware.servletunit.ServletUnitClient;

public class BucketsServletTest extends MockMongoTestCaseSupport {

	private ServletRunner sr;

	private static final Properties defaultProps = new Properties();
	static {
		try {
			defaultProps.load(Main.class
					.getResourceAsStream("defaults.properties"));
			defaultProps.put("db.uri", MockMongo.DEFAULT_URI.toString());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static class TestBucketsServlet extends BucketsServlet {
		private static final long serialVersionUID = 1L;

		public TestBucketsServlet() {
			super(defaultProps);
		}

	}

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		sr = new ServletRunner();
		sr.registerServlet("myServlet/*", TestBucketsServlet.class.getName());

	}

	private BasicBSONObject prepareBucket(String bucketId, String GET,
			String POST, String PUT) {
		BasicBSONObject o = new BasicBSONObject("_id", bucketId).append("GET",
				GET).append("POST", POST).append("PUT", PUT);
		prepareMockData("test.v7.buckets", o);
		return o;
	}

	public void testEchoPutGET() throws IOException, SAXException {

		ServletUnitClient sc = sr.newClient();
		{
			WebRequest request = new GetMethodWebRequest(
					"http://test/myServlet/1");
			request.setParameter("sha", "1234");
			try {
				sc.getResponse(request);
				fail("bucket not found => 404");
			} catch (HttpNotFoundException e) {
				assertEquals("Bucket '1' not found", e.getResponseMessage());
			}

		}

		prepareBucket("1", "EchoPut", null, null);
		{
			WebRequest request = new GetMethodWebRequest(
					"http://test/myServlet/1");
			request.setParameter("sha", "1234");
			try {
				sc.getResponse(request);
				fail("bucket not found => 404");
			} catch (HttpNotFoundException e) {
				assertEquals(
						"Bucket '1' does not have a file matching digest '1234'",
						e.getResponseMessage());
			}
		}

		MongoContentStorage storage = new MongoContentStorage(getMongo().getDB(
				"test"));
		ContentSHA sha = storage.storeContent(new ByteArrayInputStream("test"
				.getBytes()));
		{
			WebRequest request = new GetMethodWebRequest(
					"http://test/myServlet/1");
			request.setParameter("sha", sha.getDigest());
			request.setParameter("filename", "a.txt");
			WebResponse response = sc.getResponse(request);
			assertEquals("test", response.getText());
			assertEquals(sha.getDigest(), response.getHeaderField("ETag"));
			assertEquals(4, response.getContentLength());
			assertEquals("attachment; filename=\"a.txt\"", response
					.getHeaderField("Content-Disposition"));
		}

		{
			WebRequest request = new GetMethodWebRequest(
					"http://test/myServlet/1");
			request.setParameter("sha", sha.getDigest());
			request.setHeaderField("If-None-Match", sha.getDigest());
			WebResponse response = sc.getResponse(request);
			assertEquals(HttpServletResponse.SC_NOT_MODIFIED, response
					.getResponseCode());
		}

	}

	public void testEchoPutPUT() throws IOException, SAXException {

		ServletUnitClient sc = sr.newClient();
		{
			WebRequest request = new PutMethodWebRequest(
					"http://test/myServlet/1", new ByteArrayInputStream(
							"testPUT".getBytes()), "text/plain");
			try {
				sc.getResponse(request);
				fail("bucket not found => 404");
			} catch (HttpNotFoundException e) {
				assertEquals("Bucket '1' not found", e.getResponseMessage());
			}

		}

		prepareBucket("1", "EchoPut", null, null);
		{
			WebRequest request = new PutMethodWebRequest(
					"http://test/myServlet/1", new ByteArrayInputStream(
							"testPUT".getBytes()), "text/plain");
			request.setParameter("sha", "1234");
			try {
				sc.getResponse(request);
				fail("uploads not allowed => 405");
			} catch (HttpException e) {
				assertEquals(HttpServletResponse.SC_METHOD_NOT_ALLOWED, e
						.getResponseCode());
			}
		}

		prepareBucket("2", "EchoPut", null, "EchoPut");

		{
			WebRequest request = new PutMethodWebRequest(
					"http://test/myServlet/2", new ByteArrayInputStream(
							"testPUT".getBytes()), "text/plain");
			WebResponse response = sc.getResponse(request);
			assertEquals(DigestUtils.shaHex("testPUT".getBytes()), response
					.getText());

			assertMockMongoContainsDocument("test.v7files.content", DigestUtils
					.sha("testPUT".getBytes()));

			WebRequest get = new GetMethodWebRequest("http://test/myServlet/2");
			get.setParameter("sha", response.getText());
			assertEquals("testPUT", sc.getResponse(get).getText());
		}

	}

	public void testFormPostGET() throws IOException, SAXException {

		BasicBSONObject bucket = prepareBucket("1", "FormPost", null, null);
		MongoContentStorage storage = new MongoContentStorage(getMongo().getDB(
				"test"));
		ContentSHA sha = storage.storeContent(new ByteArrayInputStream("test"
				.getBytes()));
		ServletUnitClient sc = sr.newClient();
		{
			WebRequest request = new GetMethodWebRequest(
					"http://test/myServlet/1");
			request.setParameter("sha", sha.getDigest());
			try {
				sc.getResponse(request);
				fail("bucket not found => 404");
			} catch (HttpNotFoundException e) {
				assertEquals(String.format(
						"Bucket '1' does not have a file matching digest '%s'",
						sha.getDigest()), e.getResponseMessage());
			}
		}

		bucket.append("FormPost", new BasicBSONObject("data",
				new BasicBSONObject("files", new BasicBSONObject("file",
						new BasicBSONObject("filename", "a.txt").append("sha",
								sha.getSHA())))));

		{
			WebRequest request = new GetMethodWebRequest(
					"http://test/myServlet/1");
			request.setParameter("sha", sha.getDigest());
			WebResponse response = sc.getResponse(request);
			assertEquals("test", response.getText());
			assertEquals(sha.getDigest(), response.getHeaderField("ETag"));
			assertEquals(4, response.getContentLength());
			assertEquals("attachment; filename=\"a.txt\"", response
					.getHeaderField("Content-Disposition"));
		}

		{
			WebRequest request = new GetMethodWebRequest(
					"http://test/myServlet/1");
			request.setParameter("sha", sha.getDigest());
			request.setParameter("filename", "x.txt");
			WebResponse response = sc.getResponse(request);
			assertEquals("test", response.getText());
			assertEquals(sha.getDigest(), response.getHeaderField("ETag"));
			assertEquals(4, response.getContentLength());
			assertEquals("attachment; filename=\"x.txt\"", response
					.getHeaderField("Content-Disposition"));
		}

		{
			WebRequest request = new GetMethodWebRequest(
					"http://test/myServlet/1");
			request.setParameter("sha", sha.getDigest());
			request.setHeaderField("If-None-Match", sha.getDigest());
			WebResponse response = sc.getResponse(request);
			assertEquals(HttpServletResponse.SC_NOT_MODIFIED, response
					.getResponseCode());
		}

	}

}