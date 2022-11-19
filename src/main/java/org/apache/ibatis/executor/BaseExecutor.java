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
package org.apache.ibatis.executor;

import static org.apache.ibatis.executor.ExecutionPlaceholder.EXECUTION_PLACEHOLDER;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.cache.impl.PerpetualCache;
import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.executor.statement.StatementUtil;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.logging.LogFactory;
import org.apache.ibatis.logging.jdbc.ConnectionLogger;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.ParameterMode;
import org.apache.ibatis.mapping.StatementType;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.factory.ObjectFactory;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.LocalCacheScope;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.transaction.Transaction;
import org.apache.ibatis.type.TypeHandlerRegistry;

/**
 * 参考网址：https://blog.csdn.net/qq_38925911/article/details/115614765
 *
 * BaseExecutor是基础执行器，包括一级缓存逻辑也在此实现，BaseExecutor 是 Excutor接口的抽象实现类，主要提供了缓存管理和事务管理的基本功能，
 * 其实现了接口Executor的部分方法，
 * SimpleExecutor 继承了BaseExecutor抽象类，是Mybatis提供的最简单的Executor接口实现；
 * ReuseExecutor 继承了BaseExecutor抽象类，提供了对Statement重用的功能；
 * BatchExecutor 继承了BaseExecutor抽象类，实现了批处理多条 SQL 语句的功能；
 * ClosedExecutor 继承了BaseExecutor抽象类，且是ResultLoaderMap类中的一个内部类，用于实现懒加载相关逻辑；
 * CachingExecutor 类直接实现Excutor接口，是装饰器类，主要增强缓存相关功能。
 *
 */
public abstract class BaseExecutor implements Executor {

  private static final Log log = LogFactory.getLog(BaseExecutor.class);
  // Transaction对象，实现事务的提交、回滚和关闭操作
  protected Transaction transaction;
  // 封装了真正的 Executor对象
  protected Executor wrapper;

  /**
   * 定义线程安全队列，此类继承和实现如下
   *
   * public class ConcurrentLinkedQueue<E> extends AbstractQueue<E> implements Queue<E>, java.io.Serializable {}
   * 延迟加载队列。一个基于链接节点的无界线程安全队列。此队列按照 FIFO（先进先出）原则对元素进行排序。当多个线程共享访问一个公共 collection 时，
   * ConcurrentLinkedQueue 是一个恰当的选择。此队列不允许使用 null元素。该变量主要是存储一些可以延时加载的变量对象，且可供多线程使用
   */
  protected ConcurrentLinkedQueue<DeferredLoad> deferredLoads;

  // 一级缓存，用于缓存该 Executor对象查询结果集映射得到的结果对象
  protected PerpetualCache localCache;
  // Callable查询的输出参数缓存
  protected PerpetualCache localOutputParameterCache;
  // mybatis 的配置信息，全局唯一配置对象
  protected Configuration configuration;
  // 查询的深度，用来记录嵌套查询的层数，分析 DefaultResultSetHandler时介绍过的嵌套查询
  protected int queryStack;
  // 标识该执行器是否已经关闭
  private boolean closed;

  protected BaseExecutor(Configuration configuration, Transaction transaction) {
    this.transaction = transaction;
    this.deferredLoads = new ConcurrentLinkedQueue<>();
    this.localCache = new PerpetualCache("LocalCache");
    this.localOutputParameterCache = new PerpetualCache("LocalOutputParameterCache");
    this.closed = false;
    this.configuration = configuration;
    this.wrapper = this;
  }

  @Override
  public Transaction getTransaction() {
    if (closed) {
      throw new ExecutorException("Executor was closed.");
    }
    return transaction;
  }

  @Override
  public void close(boolean forceRollback) {
    try {
      try {
        rollback(forceRollback);
      } finally {
        if (transaction != null) {
          transaction.close();
        }
      }
    } catch (SQLException e) {
      // Ignore.  There's nothing that can be done at this point.
      log.warn("Unexpected exception on closing transaction.  Cause: " + e);
    } finally {
      transaction = null;
      deferredLoads = null;
      localCache = null;
      localOutputParameterCache = null;
      closed = true;
    }
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  /**
   * 更新数据库数据，INSERT/UPDATE/DELETE三种操作都会调用该方法
   * @param ms 映射语句
   * @param parameter 参数对象
   * @return 数据库操作结果
   * @throws SQLException
   */
  @Override
  public int update(MappedStatement ms, Object parameter) throws SQLException {
    ErrorContext.instance().resource(ms.getResource())
            .activity("executing an update").object(ms.getId());
    if (closed) {
      // 执行器已经关闭
      throw new ExecutorException("Executor was closed.");
    }
    // 清理本地缓存
    clearLocalCache();
    // 返回调用子类进行操作
    return doUpdate(ms, parameter);
  }

  @Override
  public List<BatchResult> flushStatements() throws SQLException {
    return flushStatements(false);
  }

  public List<BatchResult> flushStatements(boolean isRollBack) throws SQLException {
    if (closed) {
      throw new ExecutorException("Executor was closed.");
    }
    return doFlushStatements(isRollBack);
  }

  /**
   * 执行查询操作
   * @param ms 映射语句对象
   * @param parameter 参数对象
   * @param rowBounds 翻页限制
   * @param resultHandler 结果处理器
   * @param <E> 输出结果类型
   * @return 查询结果
   * @throws SQLException
   */
  /* SqlSession.selectList 会调用此方法
  在该方法中，首先判断当前执行器是否已经关闭；然后再根据 queryStack 查询层数和 flushCache属性判断，是否需要清除一级缓存；然后再判断结果处理器resultHandler是否为空，为空的话，尝试从缓存中查询数据，
  否则直接为空，后续从数据库查询数据；然后再根据缓存中是否有数据，如果存在数据，且是存储过程或函数类型则执行handleLocallyCachedOutputParameters()方法，如果存在数据，
  不是存储过程或函数类型就直接返回，如果缓存中没有数据就直接通过queryFromDatabase()方法从数据库查询数据，其中又通过doQuery()方法实现查询逻辑；后续在通过DeferredLoad类实现嵌套查询的延时加载功能*/

  @Override
  public <E> List<E> query(MappedStatement ms, Object parameter, RowBounds rowBounds, ResultHandler resultHandler) throws SQLException {
    // 得到绑定sql，并将参数对象与sql语句的#{}一一对应
    BoundSql boundSql = ms.getBoundSql(parameter);
    // 生成缓存的键，获取cacheKey供缓存，包含完整的语句、参数等，确保CacheKey的唯一性
    CacheKey key = createCacheKey(ms, parameter, rowBounds, boundSql);
    return query(ms, parameter, rowBounds, resultHandler, key, boundSql);
  }

  /**
   * 查询数据库中的数据
   * @param ms 映射语句
   * @param parameter 参数对象
   * @param rowBounds 翻页限制条件
   * @param resultHandler 结果处理器
   * @param key 缓存的键
   * @param boundSql 查询语句
   * @param <E> 结果类型
   * @return 结果列表
   * @throws SQLException
   */
  @SuppressWarnings("unchecked")
  @Override
  public <E> List<E> query(MappedStatement ms, Object parameter, RowBounds rowBounds, ResultHandler resultHandler, CacheKey key, BoundSql boundSql) throws SQLException {
    ErrorContext.instance().resource(ms.getResource()).activity("executing a query").object(ms.getId());
    // 如果 当前执行器 已经关闭就抛出异常
    if (closed) {
      // 执行器已经关闭
      throw new ExecutorException("Executor was closed.");
    }
    // 判断是否清除本地缓存，但仅仅查询堆栈为 0 才清除，为了处理递归调用
    if (queryStack == 0 && ms.isFlushCacheRequired()) {
      // 清除一级缓存
      clearLocalCache();
    }
    List<E> list;
    try {
      // 本地缓存记录自增，这样递归调用到上面的时候就不会再清局部缓存了
      queryStack++;
      // 尝试从本地缓存获取结果
      list = resultHandler == null ? (List<E>) localCache.getObject(key) : null;
      if (list != null) {
        // 本地缓存中有结果，则对于CALLABLE语句还需要绑定到IN/INOUT参数上，针对存储过程调用的处理 其功能是 在一级缓存命中时，获取缓存中保存的输出类型参数，并设到用户传入的实参（ parameter ）对象中。
        handleLocallyCachedOutputParameters(ms, key, parameter, boundSql);
      } else {
        // 本地缓存没有结果，故需要查询数据库
        list = queryFromDatabase(ms, parameter, rowBounds, resultHandler, key, boundSql);
      }
    } finally {
      // 清空堆栈操作
      queryStack--;
    }
    if (queryStack == 0) {
      // 懒加载操作的处理
      for (DeferredLoad deferredLoad : deferredLoads) {
        deferredLoad.load();
      }
      // 清空延迟加载队列
      deferredLoads.clear();
      // 如果本地缓存的作用域为STATEMENT，则立刻清除本地缓存
      if (configuration.getLocalCacheScope() == LocalCacheScope.STATEMENT) {
        clearLocalCache();
      }
    }
    return list;
  }

  @Override
  public <E> Cursor<E> queryCursor(MappedStatement ms, Object parameter, RowBounds rowBounds) throws SQLException {
    BoundSql boundSql = ms.getBoundSql(parameter);
    return doQueryCursor(ms, parameter, rowBounds, boundSql);
  }

  @Override
  public void deferLoad(MappedStatement ms, MetaObject resultObject, String property, CacheKey key, Class<?> targetType) {
    if (closed) {
      throw new ExecutorException("Executor was closed.");
    }
    DeferredLoad deferredLoad = new DeferredLoad(resultObject, property, key, localCache, configuration, targetType);
    if (deferredLoad.canLoad()) {
      deferredLoad.load();
    } else {
      deferredLoads.add(new DeferredLoad(resultObject, property, key, localCache, configuration, targetType));
    }
  }

  /**
   * 生成查询的缓存的键
   * @param ms 映射语句对象
   * @param parameterObject 参数对象
   * @param rowBounds 翻页限制
   * @param boundSql 解析结束后的SQL语句
   * @return 生成的键值
   *
   *    createCacheKey()方法
   *    创建缓存中使用的key，即CacheKey实例对象。创建的CacheKey实例对象，由下列参数决定其唯一性，
   *    即相同的查询，相关的参数，生成的CacheKey实例唯一且不变。依据的参数如下：
   *    1、MappedStatement实例的ID
   *    2、RowBounds实例的offset和limit属性
   *    3、BoundSql实例的sql属性
   *    4、查询条件的参数值
   *    5、连接数据库的环境ID
   *    MappedStatement（一般为xml中定义）、parameterObject（传递给xml的参数）、
   *    rowBounds（分页参数）、boundSql（最终sql，由MappedStatement和parameterObject决定）
   *
   * 	//还有如果一个查询的 id、分页组件中的 offset 和 limit、sql 语句、参数 都保持不变，
   * 	//那么这个查询产生的 CacheKey一定是不变的。在一个 SqlSession 的生命周期内，二次同样的查询 CacheKey 是一样的
   */
  @Override
  public CacheKey createCacheKey(MappedStatement ms, Object parameterObject, RowBounds rowBounds, BoundSql boundSql) {
    if (closed) {
      throw new ExecutorException("Executor was closed.");
    }
    // 创建CacheKey，并将所有查询参数依次更新写入
    CacheKey cacheKey = new CacheKey();
    cacheKey.update(ms.getId());
    cacheKey.update(rowBounds.getOffset());
    cacheKey.update(rowBounds.getLimit());
    cacheKey.update(boundSql.getSql());
    List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();
    TypeHandlerRegistry typeHandlerRegistry = ms.getConfiguration().getTypeHandlerRegistry();
    // mimic DefaultParameterHandler logic
    for (ParameterMapping parameterMapping : parameterMappings) {
      if (parameterMapping.getMode() != ParameterMode.OUT) {
        Object value;
        String propertyName = parameterMapping.getProperty();
        if (boundSql.hasAdditionalParameter(propertyName)) {
          value = boundSql.getAdditionalParameter(propertyName);
        } else if (parameterObject == null) {
          value = null;
        } else if (typeHandlerRegistry.hasTypeHandler(parameterObject.getClass())) {
          value = parameterObject;
        } else {
          MetaObject metaObject = configuration.newMetaObject(parameterObject);
          value = metaObject.getValue(propertyName);
        }
        cacheKey.update(value);
      }
    }
    if (configuration.getEnvironment() != null) {
      // issue #176
      cacheKey.update(configuration.getEnvironment().getId());
    }
    return cacheKey;
  }

  @Override
  public boolean isCached(MappedStatement ms, CacheKey key) {
    return localCache.getObject(key) != null;
  }

  @Override
  public void commit(boolean required) throws SQLException {
    if (closed) {
      throw new ExecutorException("Cannot commit, transaction is already closed");
    }
    clearLocalCache();
    flushStatements();
    if (required) {
      transaction.commit();
    }
  }

  @Override
  public void rollback(boolean required) throws SQLException {
    if (!closed) {
      try {
        clearLocalCache();
        flushStatements(true);
      } finally {
        if (required) {
          transaction.rollback();
        }
      }
    }
  }

  @Override
  public void clearLocalCache() {
    if (!closed) {
      localCache.clear();
      localOutputParameterCache.clear();
    }
  }

  protected abstract int doUpdate(MappedStatement ms, Object parameter)
      throws SQLException;

  protected abstract List<BatchResult> doFlushStatements(boolean isRollback)
      throws SQLException;

  protected abstract <E> List<E> doQuery(MappedStatement ms, Object parameter, RowBounds rowBounds, ResultHandler resultHandler, BoundSql boundSql)
      throws SQLException;

  protected abstract <E> Cursor<E> doQueryCursor(MappedStatement ms, Object parameter, RowBounds rowBounds, BoundSql boundSql)
      throws SQLException;

  protected void closeStatement(Statement statement) {
    if (statement != null) {
      try {
        statement.close();
      } catch (SQLException e) {
        // ignore
      }
    }
  }

  /**
   * Apply a transaction timeout.
   * @param statement a current statement
   * @throws SQLException if a database access error occurs, this method is called on a closed <code>Statement</code>
   * @since 3.4.0
   * @see StatementUtil#applyTransactionTimeout(Statement, Integer, Integer)
   */
  protected void applyTransactionTimeout(Statement statement) throws SQLException {
    StatementUtil.applyTransactionTimeout(statement, statement.getQueryTimeout(), transaction.getTimeout());
  }

  private void handleLocallyCachedOutputParameters(MappedStatement ms, CacheKey key, Object parameter, BoundSql boundSql) {
    if (ms.getStatementType() == StatementType.CALLABLE) {
      final Object cachedParameter = localOutputParameterCache.getObject(key);
      if (cachedParameter != null && parameter != null) {
        final MetaObject metaCachedParameter = configuration.newMetaObject(cachedParameter);
        final MetaObject metaParameter = configuration.newMetaObject(parameter);
        for (ParameterMapping parameterMapping : boundSql.getParameterMappings()) {
          if (parameterMapping.getMode() != ParameterMode.IN) {
            final String parameterName = parameterMapping.getProperty();
            final Object cachedValue = metaCachedParameter.getValue(parameterName);
            metaParameter.setValue(parameterName, cachedValue);
          }
        }
      }
    }
  }

  /**
   * 从数据库中查询结果
   * @param ms 映射语句
   * @param parameter 参数对象
   * @param rowBounds 翻页限制条件
   * @param resultHandler 结果处理器
   * @param key 缓存的键
   * @param boundSql 查询语句
   * @param <E> 结果类型
   * @return 结果列表
   * @throws SQLException
   */
  private <E> List<E> queryFromDatabase(MappedStatement ms, Object parameter, RowBounds rowBounds, ResultHandler resultHandler, CacheKey key, BoundSql boundSql) throws SQLException {
    List<E> list;
    // 向缓存中增加占位符，表示正在查询
    localCache.putObject(key, EXECUTION_PLACEHOLDER);
    try {
      list = doQuery(ms, parameter, rowBounds, resultHandler, boundSql);
    } finally {
      // 删除占位符
      localCache.removeObject(key);
    }
    // 将查询结果写入缓存
    localCache.putObject(key, list);
    // 如果是存储过程，OUT参数也加入缓存
    /*
      public enum StatementType {
        STATEMENT, PREPARED, CALLABLE
        声明       准备好的    可调用的
      }
    */
    if (ms.getStatementType() == StatementType.CALLABLE) {
      localOutputParameterCache.putObject(key, parameter);
    }
    return list;
  }

  /**
   * 获取一个Connection对象。mybatis在获取连接的时候，会根据日志的打印级别来判断是否会创建一个代理类。到这里就基本可以猜到，在代理类中，mybatis会去打印这个sql的语句。
   *
   * @param statementLog 日志对象
   * @return Connection对象
   * @throws SQLException
   */
  protected Connection getConnection(Log statementLog) throws SQLException {
    // 获取连接
    Connection connection = transaction.getConnection();
    // 如果是Debug级别，创建一个代理类
    if (statementLog.isDebugEnabled()) { // 启用调试日志
      // 生成Connection对象的具有日志记录功能的代理对象ConnectionLogger对象
      return ConnectionLogger.newInstance(connection, statementLog, queryStack);
    } else {
      // 返回原始的Connection对象
      return connection;
    }
  }

  @Override
  public void setExecutorWrapper(Executor wrapper) {
    this.wrapper = wrapper;
  }

  // 内部类 DeferredLoad， 负责从一级缓存中延迟加载结果对象，并赋给外层对象
  private static class DeferredLoad {
    // 结果对象(外层对象)对应的MetaObject对象
    private final MetaObject resultObject;
    // 延迟加载的属性名称
    private final String property;
    // 延迟加载的属性类型
    private final Class<?> targetType;
    // 结果对象在缓存中的 key
    private final CacheKey key;
    // 一级缓存
    private final PerpetualCache localCache;
    // 默认对象工厂
    private final ObjectFactory objectFactory;
    // 将结果从集合类型转为对象类型的辅助类
    private final ResultExtractor resultExtractor;

    // issue #781
    public DeferredLoad(MetaObject resultObject,
                        String property,
                        CacheKey key,
                        PerpetualCache localCache,
                        Configuration configuration,
                        Class<?> targetType) {
      this.resultObject = resultObject;
      this.property = property;
      this.key = key;
      this.localCache = localCache;
      this.objectFactory = configuration.getObjectFactory();
      this.resultExtractor = new ResultExtractor(configuration, objectFactory);
      this.targetType = targetType;
    }

    // 用于检测缓存对象是否已经加载到缓存中,在缓存中找到，不空且不为占位符，代表可以加载
    public boolean canLoad() {
      return localCache.getObject(key) != null && localCache.getObject(key) != EXECUTION_PLACEHOLDER;
    }

    // 负责从缓存中获取对象，并将其设置到外层对象中
    public void load() {
      @SuppressWarnings("unchecked")
      // we suppose we get back a List
      List<Object> list = (List<Object>) localCache.getObject(key);
      Object value = resultExtractor.extractObjectFromList(list, targetType);
      resultObject.setValue(property, value);
    }

  }

}
