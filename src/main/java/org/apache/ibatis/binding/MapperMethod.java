/**
 *    Copyright 2009-2019 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.binding;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.ibatis.annotations.Flush;
import org.apache.ibatis.annotations.MapKey;
import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.mapping.StatementType;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.ParamNameResolver;
import org.apache.ibatis.reflection.TypeParameterResolver;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.session.SqlSession;

/**
 * 类的作用：
 *
 * 记录 mapper 接口中方法对应的信息，主要是 SqlCommand 和 MethodSignature
 * SqlCommand：记录了当前 sql语句的名称和类型
 * MethodSignature：记录当前接口方法的 入参类型和反参信息
 */
public class MapperMethod {

  // 记录了sql的名称和类型
  private final SqlCommand command;
  // 对应的方法签名
  private final MethodSignature method;

  /**
   * MapperMethod的构造方法
   *
   * @param mapperInterface 映射接口
   * @param method 映射接口中的具体方法
   * @param config 配置信息Configuration
   */
  public MapperMethod(Class<?> mapperInterface, Method method, Configuration config) {
    this.command = new SqlCommand(config, mapperInterface, method);
    this.method = new MethodSignature(config, mapperInterface, method);
  }

  /**
   * 执行映射接口中的方法
   * @param sqlSession sqlSession接口的实例，通过它可以进行数据库的操作
   * @param args 执行接口方法时传入的参数
   * @return 数据库操作结果
   */
  public Object execute(SqlSession sqlSession, Object[] args) {
    Object result;
    // 根据SQL语句类型，执行不同操作
    switch (command.getType()) {
      // 如果是插入语句
      case INSERT: {
        // 将参数顺序与实参对应好
        Object param = method.convertArgsToSqlCommandParam(args);
        // 执行操作并返回结果
        result = rowCountResult(sqlSession.insert(command.getName(), param));
        break;
      }
      // 如果是更新语句
      case UPDATE: {
        // 将参数顺序与实参对应好
        Object param = method.convertArgsToSqlCommandParam(args);
        // 执行操作并返回结果
        result = rowCountResult(sqlSession.update(command.getName(), param));
        break;
      }
      // 如果是删除语句MappedStatement
      case DELETE: {
        // 将参数顺序与实参对应好
        Object param = method.convertArgsToSqlCommandParam(args);
        // 执行操作并返回结果
        result = rowCountResult(sqlSession.delete(command.getName(), param));
        break;
      }
      // 如果是查询语句
      case SELECT:
        // 方法返回值为void，且有结果处理器
        if (method.returnsVoid() && method.hasResultHandler()) {
          // 使用结果处理器执行查询
          executeWithResultHandler(sqlSession, args);
          result = null;
        }
        // 多条结果查询
        else if (method.returnsMany()) {
          result = executeForMany(sqlSession, args);
        }
        // Map结果查询
        else if (method.returnsMap()) {
          result = executeForMap(sqlSession, args);
        }
        // 游标类型结果查询
        else if (method.returnsCursor()) {
          result = executeForCursor(sqlSession, args);
        }
        // 单条结果查询
        else {
          Object param = method.convertArgsToSqlCommandParam(args);
          result = sqlSession.selectOne(command.getName(), param);
          if (method.returnsOptional()
              && (result == null || !method.getReturnType().equals(result.getClass()))) {
            result = Optional.ofNullable(result);
          }
        }
        break;
      // 清空缓存语句
      case FLUSH:
        result = sqlSession.flushStatements();
        break;
      // 未知语句类型，抛出异常
      default:
        throw new BindingException("Unknown execution method for: " + command.getName());
    }
    if (result == null && method.getReturnType().isPrimitive() && !method.returnsVoid()) {
      // 查询结果为null,但返回类型为基本类型。因此返回变量无法接收查询结果，抛出异常。
      throw new BindingException("Mapper method '" + command.getName()
          + " attempted to return null from a method with a primitive return type (" + method.getReturnType() + ").");
    }
    return result;
  }

  private Object rowCountResult(int rowCount) {
    final Object result;
    if (method.returnsVoid()) {
      result = null;
    } else if (Integer.class.equals(method.getReturnType()) || Integer.TYPE.equals(method.getReturnType())) {
      result = rowCount;
    } else if (Long.class.equals(method.getReturnType()) || Long.TYPE.equals(method.getReturnType())) {
      result = (long)rowCount;
    } else if (Boolean.class.equals(method.getReturnType()) || Boolean.TYPE.equals(method.getReturnType())) {
      result = rowCount > 0;
    } else {
      throw new BindingException("Mapper method '" + command.getName() + "' has an unsupported return type: " + method.getReturnType());
    }
    return result;
  }

  private void executeWithResultHandler(SqlSession sqlSession, Object[] args) {
    MappedStatement ms = sqlSession.getConfiguration().getMappedStatement(command.getName());
    if (!StatementType.CALLABLE.equals(ms.getStatementType())
        && void.class.equals(ms.getResultMaps().get(0).getType())) {
      throw new BindingException("method " + command.getName()
          + " needs either a @ResultMap annotation, a @ResultType annotation,"
          + " or a resultType attribute in XML so a ResultHandler can be used as a parameter.");
    }
    Object param = method.convertArgsToSqlCommandParam(args);
    if (method.hasRowBounds()) {
      RowBounds rowBounds = method.extractRowBounds(args);
      sqlSession.select(command.getName(), param, rowBounds, method.extractResultHandler(args));
    } else {
      sqlSession.select(command.getName(), param, method.extractResultHandler(args));
    }
  }

  /**
   * 执行多条数据结果返回
   *
   * @param sqlSession TODO
   * @param args 当前接口方法的入参
   * @param <E>
   * @return
   */
  private <E> Object executeForMany(SqlSession sqlSession, Object[] args) {
    List<E> result;
    // 解析参数名称和参数值，参数在数据存在时返回的 Map<String,Object> 类型，不存在数据时返回的是 null
    Object param = method.convertArgsToSqlCommandParam(args);
    // 当前方法是否有 RowBounds.class 类型的参数，TODO 目前不知道  RowBounds.class 的作用，猜测是 存储过程使用
    if (method.hasRowBounds()) {
      RowBounds rowBounds = method.extractRowBounds(args);
      result = sqlSession.selectList(command.getName(), param, rowBounds);
    }
    // 执行普通的 查询逻辑
    else {
      // command.name 记录的是 MappedStatement的ID，param 为 参数名称和参数值组成的map
      result = sqlSession.selectList(command.getName(), param);
    }
    // issue #510 Collections & arrays support
    if (!method.getReturnType().isAssignableFrom(result.getClass())) {
      if (method.getReturnType().isArray()) {
        return convertToArray(result);
      } else {
        return convertToDeclaredCollection(sqlSession.getConfiguration(), result);
      }
    }
    return result;
  }

  private <T> Cursor<T> executeForCursor(SqlSession sqlSession, Object[] args) {
    Cursor<T> result;
    Object param = method.convertArgsToSqlCommandParam(args);
    if (method.hasRowBounds()) {
      RowBounds rowBounds = method.extractRowBounds(args);
      result = sqlSession.selectCursor(command.getName(), param, rowBounds);
    } else {
      result = sqlSession.selectCursor(command.getName(), param);
    }
    return result;
  }

  private <E> Object convertToDeclaredCollection(Configuration config, List<E> list) {
    Object collection = config.getObjectFactory().create(method.getReturnType());
    MetaObject metaObject = config.newMetaObject(collection);
    metaObject.addAll(list);
    return collection;
  }

  @SuppressWarnings("unchecked")
  private <E> Object convertToArray(List<E> list) {
    Class<?> arrayComponentType = method.getReturnType().getComponentType();
    Object array = Array.newInstance(arrayComponentType, list.size());
    if (arrayComponentType.isPrimitive()) {
      for (int i = 0; i < list.size(); i++) {
        Array.set(array, i, list.get(i));
      }
      return array;
    } else {
      return list.toArray((E[])array);
    }
  }

  private <K, V> Map<K, V> executeForMap(SqlSession sqlSession, Object[] args) {
    Map<K, V> result;
    Object param = method.convertArgsToSqlCommandParam(args);
    if (method.hasRowBounds()) {
      RowBounds rowBounds = method.extractRowBounds(args);
      result = sqlSession.selectMap(command.getName(), param, method.getMapKey(), rowBounds);
    } else {
      result = sqlSession.selectMap(command.getName(), param, method.getMapKey());
    }
    return result;
  }

  /**
   * 当查询不存在的map下的key时抛出异常
   * @param <V>
   */
  public static class ParamMap<V> extends HashMap<String, V> {

    private static final long serialVersionUID = -2212268410512043556L;

    @Override
    public V get(Object key) {
      if (!super.containsKey(key)) {
        throw new BindingException("Parameter '" + key + "' not found. Available parameters are " + keySet());
      }
      return super.get(key);
    }
  }

  /**
   * 类的作用：记录了当前 sql语句的名称 和 执行的类型
   */
  public static class SqlCommand {

    // SQL语句的名称，即 MappedStatement
    private final String name;

    // SQL语句的种类，一共分为以下六种：增、删、改、查、清缓存、未知
    private final SqlCommandType type;

    /**
     * 本方法在创建 MapperMethod 时被调用，完成创建 SqlCommand 对象并将其设置对应的属性值
     *
     * @param configuration 全局配置文件
     * @param mapperInterface mapper 接口
     * @param method mapper 接口中对应的方法
     */
    public SqlCommand(Configuration configuration, Class<?> mapperInterface, Method method) {
      // 方法名称
      final String methodName = method.getName();

      // 方法所在的类（可能是mapperInterface，也可能是mapperInterface的子类）
      final Class<?> declaringClass = method.getDeclaringClass();
      // 从这里可以看出 MappedStatement 是在启动 mybatis 时加载的
      MappedStatement ms = resolveMappedStatement(mapperInterface, methodName, declaringClass,
          configuration);
      if (ms == null) {
        if (method.getAnnotation(Flush.class) != null) {
          name = null;
          type = SqlCommandType.FLUSH;
        } else {
          throw new BindingException("Invalid bound statement (not found): "
              + mapperInterface.getName() + "." + methodName);
        }
      } else {
        name = ms.getId();
        type = ms.getSqlCommandType();
        if (type == SqlCommandType.UNKNOWN) {
          throw new BindingException("Unknown execution method for: " + name);
        }
      }
    }

    public String getName() {
      return name;
    }

    public SqlCommandType getType() {
      return type;
    }

    /**
     * 找出指定接口指定方法对应的MappedStatement对象
     *
     * @param mapperInterface 映射接口
     * @param methodName 映射接口中具体操作方法名
     * @param declaringClass 操作方法所在的类（一般是映射接口本身，也可能是映射接口的子类）
     * @param configuration 全局配置信息
     * @return MappedStatement对象
     */
    private MappedStatement resolveMappedStatement(Class<?> mapperInterface, String methodName,
        Class<?> declaringClass, Configuration configuration) {
      // 数据库操作语句的编号是：接口名.方法名
      String statementId = mapperInterface.getName() + "." + methodName;
      // configuration 保存了解析后的所有操作语句，去查找该语句
      if (configuration.hasStatement(statementId)) {
        // 从 configuration中找到了对应的语句，返回
        return configuration.getMappedStatement(statementId);
      } else if (mapperInterface.equals(declaringClass)) {
        // 说明递归调用已经到终点，但是仍然没有找到匹配的结果
        return null;
      }
      // 从方法的定义类开始，沿着父类向上寻找。找到接口类时停止
      for (Class<?> superInterface : mapperInterface.getInterfaces()) {
        if (declaringClass.isAssignableFrom(superInterface)) {
          MappedStatement ms = resolveMappedStatement(superInterface, methodName,
              declaringClass, configuration);
          if (ms != null) {
            return ms;
          }
        }
      }
      return null;
    }
  }

  /**
   * 记录当前接口方法的 入参类型和反参信息
   */
  public static class MethodSignature {

    // 返回类型是否为集合类型
    private final boolean returnsMany;
    // 返回类型是否是map
    private final boolean returnsMap;
    // 返回类型是否是空
    private final boolean returnsVoid;
    // 返回类型是否是cursor类型
    private final boolean returnsCursor;
    // 返回类型是否是optional类型
    private final boolean returnsOptional;
    // 返回类型
    private final Class<?> returnType;
    // 如果返回为map,这里记录所有的map的key
    private final String mapKey;
    // resultHandler参数的位置
    private final Integer resultHandlerIndex;
    // rowBounds参数的位置
    private final Integer rowBoundsIndex;
    // 引用参数名称解析器
    private final ParamNameResolver paramNameResolver;

    /**
     * 通过构造函数初始化所有参数
     *
     * @param configuration
     * @param mapperInterface
     * @param method
     */
    public MethodSignature(Configuration configuration, Class<?> mapperInterface, Method method) {
      // 通过 接口和接口的方法获取，当前方法的返回值类型
      Type resolvedReturnType = TypeParameterResolver.resolveReturnType(method, mapperInterface);
      // TODO 为什么只判断这两种类型，ParameterizedType 是什么类型的数据
      if (resolvedReturnType instanceof Class<?>) {
        this.returnType = (Class<?>) resolvedReturnType;
      } else if (resolvedReturnType instanceof ParameterizedType) {
        this.returnType = (Class<?>) ((ParameterizedType) resolvedReturnType).getRawType();
      } else {
        this.returnType = method.getReturnType();
      }
      // 判断返回类型并进行设置对应的值
      this.returnsVoid = void.class.equals(this.returnType);
      this.returnsMany = configuration.getObjectFactory().isCollection(this.returnType) || this.returnType.isArray();
      this.returnsCursor = Cursor.class.equals(this.returnType);
      this.returnsOptional = Optional.class.equals(this.returnType);

      // 获取方法上 MapKey 注解上的 value 值
      this.mapKey = getMapKey(method);
      this.returnsMap = this.mapKey != null;
      // 获取第一个并且唯一的 RowBounds类型的入参类型数据下标
      this.rowBoundsIndex = getUniqueParamIndex(method, RowBounds.class);
      // 获取第一个并且唯一的 ResultHandler类型的入参类型数据下标
      this.resultHandlerIndex = getUniqueParamIndex(method, ResultHandler.class);
      // 初始化 ParamNameResolver 解析器
      this.paramNameResolver = new ParamNameResolver(configuration, method);
    }

    /**
     * 解析参数名称和参数值
     *
     * @param args
     * @return 参数在数据存在时返回的 Map<String,Object> 类型，不存在数据时返回的是 null
     */
    public Object convertArgsToSqlCommandParam(Object[] args) {
      return paramNameResolver.getNamedParams(args);
    }

    public boolean hasRowBounds() {
      return rowBoundsIndex != null;
    }

    public RowBounds extractRowBounds(Object[] args) {
      return hasRowBounds() ? (RowBounds) args[rowBoundsIndex] : null;
    }

    public boolean hasResultHandler() {
      return resultHandlerIndex != null;
    }

    public ResultHandler extractResultHandler(Object[] args) {
      return hasResultHandler() ? (ResultHandler) args[resultHandlerIndex] : null;
    }

    public String getMapKey() {
      return mapKey;
    }

    public Class<?> getReturnType() {
      return returnType;
    }

    public boolean returnsMany() {
      return returnsMany;
    }

    public boolean returnsMap() {
      return returnsMap;
    }

    public boolean returnsVoid() {
      return returnsVoid;
    }

    public boolean returnsCursor() {
      return returnsCursor;
    }

    /**
     * return whether return type is {@code java.util.Optional}.
     * @return return {@code true}, if return type is {@code java.util.Optional}
     * @since 3.5.0
     */
    public boolean returnsOptional() {
      return returnsOptional;
    }

    // 返回指定参数的index
    private Integer getUniqueParamIndex(Method method, Class<?> paramType) {
      Integer index = null;
      final Class<?>[] argTypes = method.getParameterTypes();
      for (int i = 0; i < argTypes.length; i++) {
        if (paramType.isAssignableFrom(argTypes[i])) {
          if (index == null) {
            index = i;
          } else {
            throw new BindingException(method.getName() + " cannot have multiple " + paramType.getSimpleName() + " parameters");
          }
        }
      }
      return index;
    }


    private String getMapKey(Method method) {
      String mapKey = null;
      if (Map.class.isAssignableFrom(method.getReturnType())) {
        final MapKey mapKeyAnnotation = method.getAnnotation(MapKey.class);
        if (mapKeyAnnotation != null) {
          mapKey = mapKeyAnnotation.value();
        }
      }
      return mapKey;
    }
  }

}
