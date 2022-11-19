package com.github.yeecode.mybatisdemo;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * <p>
 * 文件描述（必填！！！）
 * </p>
 *
 * @author lvchao
 * @since 2022/11/14 16:17
 */
public class DemoApplication {
    public static void main(String[] args) {
        // 第一阶段：MyBatis的初始化阶段
        String resource = "mybatis-config.xml";
        // 得到配置文件的输入流
        InputStream inputStream = null;
        try {
            inputStream = Resources.getResourceAsStream(resource);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 得到 SqlSessionFactory，采用的是  DefaultSqlSessionFactory
        SqlSessionFactory sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);

        // 第二阶段：数据读写阶段
        SqlSession session = null;
        try {
            session = sqlSessionFactory.openSession();
            // 返回的就是动态代理创建的对象，即 MapperProxy 代理实现
            UserMapper userMapper = session.getMapper(UserMapper.class);
            // 组建查询参数
            User userParam = new User();
            userParam.setSchoolName("Sunny School");
            // 调用接口展开数据库操作
            List<User> userList =  userMapper.queryUserBySchoolName(userParam);
            // 打印查询结果
            for (User user : userList) {
                System.out.println("name : " + user.getName() + " ;  email : " + user.getEmail());
            }

        } finally {
            session.close();
        }
    }
}
