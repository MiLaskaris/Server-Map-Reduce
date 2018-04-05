package com.aueb.distributed.mapreduce.service;
import java.util.UUID;


public class RequestDifferentiatorSeviceImp implements RequestDifferentiatorService {

	@Override
	public String getRequestID() {
		String requestId= UUID.randomUUID().toString();
		return requestId;
	}
}
