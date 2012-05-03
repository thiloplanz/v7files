package v7db.files.milton;

import com.meterware.httpunit.HeaderOnlyWebRequest;

class MkColWebRequest extends HeaderOnlyWebRequest {

	MkColWebRequest(String urlString) {
		super(urlString);
		setMethod("MKCOL");
	}

}
