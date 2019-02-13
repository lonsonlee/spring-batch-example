package org.home.spring.batch.listener;

import org.home.spring.app.oracle.model.User;
import org.springframework.batch.core.ItemReadListener;

public class UserItemReadListner implements ItemReadListener<User>{

	@Override
	public void beforeRead() {
		System.out.println("Reading from file");
		
	}

	@Override
	public void afterRead(User item) {
		System.out.println("Read userName: "+item.getName());
		
	}

	@Override
	public void onReadError(Exception ex) {
		// TODO Auto-generated method stub
		
	}


}
