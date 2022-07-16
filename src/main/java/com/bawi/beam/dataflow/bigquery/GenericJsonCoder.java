package com.bawi.beam.dataflow.bigquery;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.api.client.json.GenericJson;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

// https://github.com/rishisinghal/BeamPipelineSamples/blob/master/src/main/java/com/sample/beam/df/utils/GenericJsonCoder.java
public class GenericJsonCoder<T extends GenericJson> extends CustomCoder<T> {

	private static final long serialVersionUID = 1L;

	// FAIL_ON_EMPTY_BEANS is disabled in order to handle null values
	private static final ObjectMapper MAPPER = new ObjectMapper().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

	private final Class<T> type;
	private final TypeDescriptor<T> typeDescriptor;

	public static <T extends GenericJson> GenericJsonCoder<T> of(Class<T> type) {
		return new GenericJsonCoder<>(type);
	}

	private GenericJsonCoder(Class<T> type) {
		this.type = type;
		this.typeDescriptor = TypeDescriptor.of(type);
	}

	@Override
	public void encode(T value, OutputStream outStream) throws IOException {
		String strValue = MAPPER.writeValueAsString(value);
		StringUtf8Coder.of().encode(strValue, outStream);
	}

	@Override
	public T decode(InputStream inStream) throws IOException {
		String strValue = StringUtf8Coder.of().decode(inStream);
		return MAPPER.readValue(strValue, type);
	}

	@Override
	public void verifyDeterministic() throws NonDeterministicException {
		throw new NonDeterministicException(this,
				"GenericJson can hold arbitrary instances, which may be non-deterministic.");
	}

	@Override
	public TypeDescriptor<T> getEncodedTypeDescriptor() {
		return typeDescriptor;
	}

}
