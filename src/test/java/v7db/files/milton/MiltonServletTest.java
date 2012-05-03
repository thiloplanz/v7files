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
package v7db.files.milton;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Properties;

import javax.servlet.http.HttpServletResponse;

import jmockmongo.MockMongoTestCaseSupport;

import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;
import org.xml.sax.SAXException;

import v7db.files.UnitTestSupport;

import com.meterware.httpunit.GetMethodWebRequest;
import com.meterware.httpunit.HttpNotFoundException;
import com.meterware.httpunit.PutMethodWebRequest;
import com.meterware.httpunit.WebRequest;
import com.meterware.httpunit.WebResponse;
import com.meterware.servletunit.ServletRunner;
import com.meterware.servletunit.ServletUnitClient;

public class MiltonServletTest extends MockMongoTestCaseSupport {

	private ServletRunner sr;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		Properties props = UnitTestSupport.getDefaultProperties();
		props.put("auth.provider", "demo");
		props.put("acl.provider", "global");
		props.put("acl.read", "admins");
		props.put("acl.write", "admins");
		UnitTestSupport.initConfiguration(props);

		sr = new ServletRunner();
		Hashtable<String, String> initParams = new Hashtable<String, String>();
		initParams.put("webdav.endpoint", "webdav");
		initParams.put("resource.factory.factory.class",
				ResourceFactoryFactory.class.getName());
		sr.registerServlet("myServlet/*", MiltonServlet.class.getName(),
				initParams);

	}

	public void testSimpleScenario() throws IOException, SAXException {
		ServletUnitClient sc = sr.newClient();
		sc.setAuthentication("V7Files", "admin", "admin");
		{
			WebRequest request = new MkColWebRequest("http://test/myServlet/1");
			WebResponse resp = sc.getResponse(request);
			assertEquals(HttpServletResponse.SC_CREATED, resp.getResponseCode());
		}

		{
			WebRequest request = new PutMethodWebRequest(
					"http://test/myServlet/1/test.txt",
					new ByteArrayInputStream("testPUT".getBytes()),
					"text/plain");
			WebResponse resp = sc.getResponse(request);
			assertEquals(HttpServletResponse.SC_CREATED, resp.getResponseCode());
		}
		{
			WebRequest request = new GetMethodWebRequest(
					"http://test/myServlet//1/test.txt");
			WebResponse resp = sc.getResponse(request);
			assertEquals(HttpServletResponse.SC_OK, resp.getResponseCode());
		}

	}

	public void testGET() throws IOException, SAXException {
		ServletUnitClient sc = sr.newClient();
		sc.setAuthentication("V7Files", "admin", "admin");

		{
			WebRequest request = new GetMethodWebRequest(
					"http://test/myServlet/a.txt");
			try {
				sc.getResponse(request);
				fail("file not found => 404");
			} catch (HttpNotFoundException e) {
			}
		}

		prepareMockData("test.v7files.files", new BasicBSONObject("_id",
				new ObjectId()).append("filename", "a.txt").append("parent",
				"webdav").append("in", "abcd".getBytes()));

		{
			WebRequest request = new GetMethodWebRequest(
					"http://test/myServlet/a.txt");
			WebResponse resp = sc.getResponse(request);
			assertEquals("abcd", resp.getText());
			assertEquals(HttpServletResponse.SC_OK, resp.getResponseCode());
		}
	}
}
