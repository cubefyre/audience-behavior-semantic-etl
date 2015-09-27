/**
 * SparklineData, Inc. -- http://www.sparklinedata.com/
 *
 * Scala based Audience Behavior APIs
 *
 * Copyright 2014-2015 SparklineData, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 /**
 * Created by Harish.
 */
 package org.sparkline.etl

import org.apache.spark.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan


case class AnalysisLayer(prev : Either[AnalysisLayer, Seq[Attribute]],
                          metrics : Seq[Expression]) extends Serializable {

  def this(prev : AnalysisLayer, metrics : Seq[Expression]) = this(Left(prev), metrics)
  def this(prev : Seq[Attribute], metrics : Seq[Expression]) = this(Right(prev), metrics)

  def inputSchema : Seq[Attribute] = if (prev.isLeft) prev.left.get.outputSchema else prev.right.get
  def outputSchema : Seq[Attribute] = inputSchema ++ metricsSchema
  def metricsSchema : Seq[Attribute] = metrics.map(_.asInstanceOf[NamedExpression].toAttribute)

  def namedMetrics : Seq[NamedExpression] = metrics.map(expr => Alias(expr, expr.prettyString)())

  def treeString() = generateTreeString(0, new StringBuilder).toString()

  protected def generateTreeString(depth: Int, builder: StringBuilder): StringBuilder = {
    builder.append(" " * depth)
    builder.append("Metrics = ")
    builder.append(metrics.map(_.prettyString).mkString("[", ",", "]"))
    builder.append("\n")
    if (prev.isLeft) {
      prev.left.get.generateTreeString(depth + 1, builder)
    } else {
      builder.append(" " * (depth+1))
      builder.append("InputSchema = ")
      builder.append(prev.right.get.map(_.prettyString).mkString("[", ",", "]"))
      builder.append("\n")
    }
    builder
  }

}

object MetricResolver extends Logging {

  /**
   * Resolve the given `name` string against the given attribute, returning either 0 or 1 match.
   *
   * Different from resolveAsTableColumn, this assumes `name` does NOT start with a qualifier.
   * See the comment above `candidates` variable in resolve() for semantics the returned data.
   */
  private def resolveAsColumn(
                               nameParts: Seq[String],
                               resolver: Resolver,
                               attribute: Attribute): Option[(Attribute, List[String])] = {
    if (resolver(attribute.name, nameParts.head)) {
      Option((attribute.withName(nameParts.head), nameParts.tail.toList))
    } else {
      None
    }
  }

  /**
   * Resolve the given `name` string against the given attribute, returning either 0 or 1 match.
   *
   * This assumes `name` has multiple parts, where the 1st part is a qualifier
   * (i.e. table name, alias, or subquery alias).
   * See the comment above `candidates` variable in resolve() for semantics the returned data.
   */
  private def resolveAsTableColumn(
                                    nameParts: Seq[String],
                                    resolver: Resolver,
                                    attribute: Attribute): Option[(Attribute, List[String])] = {
    assert(nameParts.length > 1)
    if (attribute.qualifiers.exists(resolver(_, nameParts.head))) {
      // At least one qualifier matches. See if remaining parts match.
      val remainingParts = nameParts.tail
      resolveAsColumn(remainingParts, resolver, attribute)
    } else {
      None
    }
  }

  protected def resolve(
                         nameParts: Seq[String],
                         input: Seq[Attribute],
                         resolver: Resolver,
                         throwErrors: Boolean = false): Option[NamedExpression] = {

    // A sequence of possible candidate matches.
    // Each candidate is a tuple. The first element is a resolved attribute, followed by a list
    // of parts that are to be resolved.
    // For example, consider an example where "a" is the table name, "b" is the column name,
    // and "c" is the struct field name, i.e. "a.b.c". In this case, Attribute will be "a.b",
    // and the second element will be List("c").
    var candidates: Seq[(Attribute, List[String])] = {
      // If the name has 2 or more parts, try to resolve it as `table.column` first.
      if (nameParts.length > 1) {
        input.flatMap { option =>
          resolveAsTableColumn(nameParts, resolver, option)
        }
      } else {
        Seq.empty
      }
    }

    // If none of attributes match `table.column` pattern, we try to resolve it as a column.
    if (candidates.isEmpty) {
      candidates = input.flatMap { candidate =>
        resolveAsColumn(nameParts, resolver, candidate)
      }
    }

    def name = UnresolvedAttribute(nameParts).name

    candidates.distinct match {
      // One match, no nested fields, use it.
      case Seq((a, Nil)) => Some(a)

      // One match, but we also need to extract the requested nested field.
      case Seq((a, nestedFields)) =>
        try {
          // The foldLeft adds GetFields for every remaining parts of the identifier,
          // and aliases it with the last part of the identifier.
          // For example, consider "a.b.c", where "a" is resolved to an existing attribute.
          // Then this will add GetField("c", GetField("b", a)), and alias
          // the final expression as "c".
          val fieldExprs = nestedFields.foldLeft(a: Expression)((c : Expression, e : String) => ExtractValue(c, Literal(e), resolver))
          val aliasName = nestedFields.last
          Some(Alias(fieldExprs, aliasName)())
        } catch {
          case a: MetricAnalysisException if !throwErrors => None
          case a: AnalysisException if !throwErrors => None
        }

      // No matches.
      case Seq() =>
        logTrace(s"Could not find $name in ${input.mkString(", ")}")
        None

      // More than one match.
      case ambiguousReferences =>
        val referenceNames = ambiguousReferences.map(_._1).mkString(", ")
        throw new MetricAnalysisException(
          s"Reference '$name' is ambiguous, could be: $referenceNames.")
    }
  }

  def resolve(expr : Expression, inputSchema : Seq[Attribute], resolver: Resolver) : Expression = expr transformUp  {
    case u @ UnresolvedAttribute(nameParts) =>
      // Leave unchanged if resolution fails.  Hopefully will be resolved next round.
      val result = withPosition(u) { resolve(nameParts, inputSchema, resolver).getOrElse(u) }
      logDebug(s"Resolving $u to $result")
      result
    case UnresolvedExtractValue(child, fieldName) if child.resolved =>
      ExtractValue(child, fieldName, resolver)
  }

  def resolve(expr : Expression, input : LogicalPlan, resolver: Resolver) : Expression =
      resolve(expr, input.output, resolver)

  def resolve(metrics : Seq[Expression], inputPlan : LogicalPlan, resolver: Resolver) : AnalysisLayer = {

    var unresolvedMetrics = metrics
    var resolvedMetrics = Seq[Expression]()
    var inputSchema = inputPlan.output
    var analysisLayer : Option[AnalysisLayer] = None
    //var (resolvedMetrics, unresolvedMetrics) = sessionMetrics.partition(_.resolved)

    while(unresolvedMetrics.size > 0) {

      val metrics = unresolvedMetrics.map { m =>
        MetricResolver.resolve(m, inputSchema, caseInsensitiveResolution)
      }

      val (r, u) = metrics.partition(_.resolved)

      if ( r.size == 0 ) {
        throw new MetricAnalysisException("Cannot resolve metric ${u(0).prettyString}")
      }

      analysisLayer =
        analysisLayer.map( a => new AnalysisLayer(a, r)).orElse(Some(new AnalysisLayer(inputSchema, r)))

      inputSchema = analysisLayer.get.outputSchema
      unresolvedMetrics = u

    }

    analysisLayer.get
  }
}
