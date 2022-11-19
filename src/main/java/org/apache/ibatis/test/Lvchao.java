package org.apache.ibatis.test;

/**
 * <p>
 * 文件描述（必填！！！）
 * </p>
 *
 * @author lvchao
 * @since 2022/11/2 13:05
 */
public class Lvchao {
    private String name;
    private Integer age;

    public Lvchao(String name, Integer age) {
        this.name = name;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "Lvchao{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}
