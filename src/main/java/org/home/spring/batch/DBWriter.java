package org.home.spring.batch;

import java.util.List;

import org.home.spring.app.oracle.model.User;
import org.home.spring.app.oracle.repository.UserRepository;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class DBWriter implements ItemWriter<User>, ItemWriteListener<User> {

	@Autowired
	private UserRepository userRepository;

	@Override
	public void beforeWrite(List<? extends User> users) {
		System.out.println("Before writing to the DB.");
		
	}
	
	@Override
	public void write(List<? extends User> users) throws Exception {
		userRepository.saveAll(users);
	}

	@Override
	public void afterWrite(List<? extends User> users) {
		System.out.println("Data Saved for Users: " + users);
	}

	@Override
	public void onWriteError(Exception exception, List<? extends User> users) {
		// TODO Auto-generated method stub
	}


}
