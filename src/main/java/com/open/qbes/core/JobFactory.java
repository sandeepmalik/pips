package com.open.qbes.core;

/**
 * Created with IntelliJ IDEA.
 * User: Sandeep Malik
 * Date: 12/8/12
 * Time: 10:45 AM
 */
public interface JobFactory {

    public <T, J extends Job<T>> J create(Class<J> jobClass) throws Exception;

}
