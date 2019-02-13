package org.home.spring.batch;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.home.spring.app.oracle.model.User;
import org.springframework.batch.core.ItemProcessListener;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class Processor implements ItemProcessor<User, User>, ItemProcessListener<User, User>{

    private static final Map<String, String> DEPT_NAMES =
            new HashMap<>();

    public Processor() {
        DEPT_NAMES.put("001", "Technology");
        DEPT_NAMES.put("002", "Operations");
        DEPT_NAMES.put("003", "Accounts");
    }

	@Override
	public void beforeProcess(User item) {
		System.out.println("Before Processing user:"+item.getName()+" DeptNumber:"+item.getDept());
	}
	
	@Override
    public User process(User user) throws Exception {
        String deptCode = user.getDept();
        String dept = DEPT_NAMES.get(deptCode);
        user.setDept(dept);
        user.setTime(new Date());
        System.out.println(String.format("Converted from [%s] to [%s]", deptCode, dept));
        return user;
    }

	@Override
	public void afterProcess(User item, User result) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onProcessError(User item, Exception e) {
		// TODO Auto-generated method stub
		
	}
}
