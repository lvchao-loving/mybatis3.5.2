/**
 *    Copyright 2009-2018 the original author or authors.
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
package org.apache.ibatis.reflection;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.binding.MapperMethod.ParamMap;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;

/**
 * 类的作用：
 *
 * 1.记录方法入参中是否含有@Param注解
 * 2.记录了参数的次序和用@Param修饰后的名称
 */
public class ParamNameResolver {

  private static final String GENERIC_NAME_PREFIX = "param";

  /**
   * <p>
   * The key is the index and the value is the name of the parameter.<br />
   * The name is obtained from {@link Param} if specified. When {@link Param} is not specified,
   * the parameter index is used. Note that this index could be different from the actual index
   * when the method has special parameters (i.e. {@link RowBounds} or {@link ResultHandler}).
   * </p>
   * <ul>
   * <li>aMethod(@Param("M") int a, @Param("N") int b) -&gt; {{0, "M"}, {1, "N"}}</li>
   * <li>aMethod(int a, int b) -&gt; {{0, "0"}, {1, "1"}}</li>
   * <li>aMethod(int a, RowBounds rb, int b) -&gt; {{0, "0"}, {2, "1"}}</li>
   * </ul>
   *
   * 凡是加了@Param注解的会单独处理，特殊参数也会单独处理
   */

  // 方法入参的参数次序表。键为参数次序，值为参数名称或者参数 @Param 注解的值
  private final SortedMap<Integer, String> names;

  // 该方法入参中是否含有@Param注解
  private boolean hasParamAnnotation;

  /**
   * 参数名解析器的构造方法
   * @param config 配置信息
   * @param method 要被分析的方法
   */
  public ParamNameResolver(Configuration config, Method method) {
    // 获取参数类型列表
    final Class<?>[] paramTypes = method.getParameterTypes();
    // 准备存取所有参数的注解，是二维数组
    final Annotation[][] paramAnnotations = method.getParameterAnnotations();
    // 记录当前参数的位置和名称 key-参数次序 value-参数的名称（该名称可能取三种值：1.用 @Param 注解修饰的 value名称 2.参数的名称 3.参数次序index，且当前优先级也为1->3）
    final SortedMap<Integer, String> map = new TreeMap<>();
    int paramCount = paramAnnotations.length;
    // 循环处理各个参数 （第一层循环为参数，第二层循环为当前参数上的说有注解遍历）
    for (int paramIndex = 0; paramIndex < paramCount; paramIndex++) {
      // 判断当前类是否是 RowBounds类或者是 ResultHandler 类，是则返回true否则返回false
      if (isSpecialParameter(paramTypes[paramIndex])) {
        // 跳过 RowBounds类型 或者 ResultHandler类型的 入参
        continue;
      }
      // 参数名称
      String name = null;
      for (Annotation annotation : paramAnnotations[paramIndex]) {
        // 找出参数的注解
        if (annotation instanceof Param) {
          // 如果注解是Param
          hasParamAnnotation = true;
          // 那就以Param中值作为参数名
          name = ((Param) annotation).value();
          break;
        }
      }
      // 走到这里说明当前参数没有使用 Param 参数
      if (name == null) {
        // 否则，保留参数的原有名称
        if (config.isUseActualParamName()) {
          name = getActualParamName(method, paramIndex);
        }
        if (name == null) {
          // 参数名称取不到，则按照参数index命名
          name = String.valueOf(map.size());
        }
      }
      map.put(paramIndex, name);
    }
    // 设置参数属性，并修改成不可修改状态
    names = Collections.unmodifiableSortedMap(map);
  }

  private String getActualParamName(Method method, int paramIndex) {
    return ParamNameUtil.getParamNames(method).get(paramIndex);
  }

  /**
   * 判断当前类是否是 RowBounds类或者是 ResultHandler 类，是则返回true否则返回false
   *
   * @param clazz
   * @return
   */
  private static boolean isSpecialParameter(Class<?> clazz) {
    return RowBounds.class.isAssignableFrom(clazz) || ResultHandler.class.isAssignableFrom(clazz);
  }

  /**
   * Returns parameter names referenced by SQL providers.
   * 给出被解析的方法中的参数名称列表
   */
  public String[] getNames() {
    return names.values().toArray(new String[0]);
  }

  /**
   * 解析参数名称和参数值
   *
   * 将被解析的方法中的参数名称列表与传入的`Object[] args`进行对应，封装成一个map 其中 key-参数的名称 value-为参数的值，
   * 其次 在返回的参数中 对于每个参数多一个默认的参数名 param1...paramN，value为对应的参数值，放入 map 集合中。
   *
   * 1.如果为空或者只有一个参数，直接返回参数
   * 2.如果有多个参数，则进行与之前解析出的参数名称进行对应，返回对应关系
   *
   * @param args
   * @return
   */
  public Object getNamedParams(Object[] args) {
    final int paramCount = names.size();
    if (args == null || paramCount == 0) {
      return null;
    } else if (!hasParamAnnotation && paramCount == 1) {
      return args[names.firstKey()];
    } else {
      final Map<String, Object> param = new ParamMap<>();
      int i = 0;
      for (Map.Entry<Integer, String> entry : names.entrySet()) {
        // 首先按照类注释中提供的key,存入一遍   【参数的@Param名称 或者 参数排序：实参值】
        // 注意，key和value交换了位置
        param.put(entry.getValue(), args[entry.getKey()]);
        // add generic param names (param1, param2, ...)
        final String genericParamName = GENERIC_NAME_PREFIX + String.valueOf(i + 1);
        // ensure not to overwrite parameter named with @Param
        // 再按照param1, param2, ...的命名方式存入一遍
        if (!names.containsValue(genericParamName)) {
          param.put(genericParamName, args[entry.getKey()]);
        }
        i++;
      }
      return param;
    }
  }
}
