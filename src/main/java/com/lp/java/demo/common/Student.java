package com.lp.java.demo.common;

/**
 * <p/>
 * <li>title: 学生实体类</li>
 * <li>@author: li.pan</li>
 * <li>Date: 2019/12/29 5:19 下午</li>
 * <li>Version: V1.0</li>
 * <li>Description: 学生类</li>
 */
public class Student {
    private int id;

    private String name;

    private int age;

    @Override
    public String toString() {
        return "Student{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", age=" + age +
                '}';
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }
}
