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

 package org.sparkline.etl.operators

/**
 * Created by Jitender on 7/13/15.
 */
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}


object CaseStatement {

  def dataFrame(input : DataFrame,
                caseBranches : Seq[Expression],
                caseAttrName : String) : DataFrame = {
    import org.apache.spark.sql.catalyst.dsl.plans._

    val inputPlan = input.queryExecution.analyzed
    val attrs = inputPlan.schema.map(f => UnresolvedAttribute(f.name))
    val caseExpr = Alias(CaseWhen(caseBranches), caseAttrName)()

    val casePlan = inputPlan.select((attrs :+ caseExpr):_*)
    new DataFrame(input.sqlContext, casePlan)
  }

  def caseUsingLikeOld(input: DataFrame,
                    whenBranches : List[(String, String, String)],
                    elseValue : String,
                      attrAlias : String): DataFrame = {

    val caseExprs : Seq[Seq[Expression]] = whenBranches.map(f => {
      val (leftOfBranch, rightofBranch, branchValue) = f
      Seq(
        Like(UnresolvedAttribute(leftOfBranch), Literal.create(rightofBranch, StringType)),
        Literal.create(branchValue, StringType)
      )
    })
    dataFrame(input, caseExprs.flatten :+ Literal.create(elseValue, StringType), attrAlias)
  }


  def caseUsingLike(
                    caseBranches : List[(String, String, String)],
                    elseValue : String,
                    caseAttrAlias : String): NamedExpression = {

    val caseBranchesExprs : Seq[Seq[Expression]] = caseBranches.map({
      case (leftOfBranch, rightofBranch, branchValue) =>
        Seq(Like(UnresolvedAttribute(leftOfBranch), Literal(rightofBranch)),
          Literal.create(branchValue, StringType))
    })
    Alias(CaseWhen(caseBranchesExprs.flatten :+ Literal.create(elseValue, StringType)), caseAttrAlias)()
  }

  def caseUsingEquals(
                    caseBranches : List[(String, String, Any)],
                    elseValue : Any, elseValueDataType: DataType,
                    caseAttrAlias : String): NamedExpression = {

    val caseBranchesExprs : Seq[Seq[Expression]] = caseBranches.map({
      case (leftOfBranch, rightofBranch, branchValue) =>
        Seq(EqualTo(UnresolvedAttribute(leftOfBranch), Literal(rightofBranch)),
          Literal(branchValue))
    })
    Alias(CaseWhen(caseBranchesExprs.flatten :+ Literal.create(elseValue, elseValueDataType)), caseAttrAlias)()
  }

  def caseNested(
                       caseBranches : List[(String, String, Any)],
                       elseAttrName : String,
                       caseAttrAlias : String): NamedExpression = {

    val caseBranchesExprs : Seq[Seq[Expression]] = caseBranches.map({
      case (leftOfBranch, rightofBranch, branchValue) =>
        Seq(EqualTo(UnresolvedAttribute(leftOfBranch), Literal(rightofBranch)),
          Literal(branchValue))
    })
    Alias(CaseWhen(caseBranchesExprs.flatten :+ UnresolvedAttribute(elseAttrName)), caseAttrAlias)()
  }

  def generateCampaignSource : NamedExpression = {

    val caseBranches: List[(String, String)] = List(
      ("", "Direct"),
      ("%google.com%", "Google"),
      ("%googleadservices%", "Google"),
      ("%cart%", "Cart"),
      ("%home-laser-hair%", "LHR Search"),
      ("%age-defying-laser%", "AgeDefSearch"),
      ("%Quest%", "Questionnaire"),
      ("%orderReview%", "OrderReview"),
      ("%yahoo%", "Yahoo"),
      ("%bing%", "Bing"),
      ("%googleadservices%", "Google")
    )

    val innerCaseBranchExprs: Seq[Seq[Expression]] = caseBranches.map({
      case (matchCondition, matchedValue) => Seq(
        And(
          And(
            IsNull(UnresolvedAttribute("sd_source_o")),
            IsNull(UnresolvedAttribute("sd_medium_o"))
          ),
          Like(UnresolvedAttribute("sd_host"), Literal(matchCondition))
        ),
        Literal(matchedValue))
    })

    val caseBranchesExpr = Seq(And(IsNull(UnresolvedAttribute("campaign_medium")), IsNull(UnresolvedAttribute("campaign_source"))),
      CaseWhen(innerCaseBranchExprs.flatten :+ Literal("Other")),
      UnresolvedAttribute("campaign_source")
    )
    Alias(CaseWhen(caseBranchesExpr), "sd_source")()

    /*
       case when (context.campaign.medium is null and context.campaign.source is null ) then
                     ( case
                         when ( sd_source is null and sd_medium is null and sd_host is null)  then 'Direct'

                         when ( sd_source is null and sd_medium is null and sd_host like '%google.com%') then 'Google'
                         when ( sd_source is null and sd_medium is null and sd_host like '%googleadservices%') then 'Google'
                         when ( sd_source is null and sd_medium is null and sd_host like '%cart%') then 'Cart'
                         when ( sd_source is null and sd_medium is null and sd_host like '%home-laser-hair%') then 'LHR Search'
                         when ( sd_source is null and sd_medium is null and sd_host like '%age-defying-laser%') then 'AgeDefSearch'
                         when ( sd_source is null and sd_medium is null and sd_host like '%Quest%') then 'Questionnaire'
                         when ( sd_source is null and sd_medium is null and sd_host like '%orderReview%') then 'OrderReview'
                         when ( sd_source is null and sd_medium is null and sd_host like '%yahoo%') then 'Yahoo'
                         when ( sd_source is null and sd_medium is null and sd_host like '%bing%') then 'Bing'
                         when ( sd_source is null and sd_medium is null and sd_query like '%gclid%') then 'Google'
                         else 'Other'
                         end )

                 else
                 context.campaign.source
                 end as sd_source,
        */
  }
  def generateCampaignMedium : NamedExpression = {

    val caseBranches: List[(String, String)] = List(
      ("", "Direct"),
      ("%google.com%", "Search"),
      ("%googleadservices%", "Adwords"),
      ("%cart%", "Cart"),
      ("%home-laser-hair%", "Search"),
      ("%age-defying-laser%", "Search"),
      ("%Quest%", "Questionnaire"),
      ("%orderReview%", "OrderReview"),
      ("%yahoo%", "Yahoo"),
      ("%bing%", "Bing"),
      ("%googleadservices%", "Adwords")
    )

    val innerCaseBranchExprs: Seq[Seq[Expression]] = caseBranches.map({
      case (matchCondition, matchedValue) => Seq(
        And(
          And(
            IsNull(UnresolvedAttribute("sd_source_o")),
            IsNull(UnresolvedAttribute("sd_medium_o"))
          ),
          Like(UnresolvedAttribute("sd_host"), Literal(matchCondition))
        ),
        Literal(matchedValue))
    })

    val caseBranchesExpr = Seq(
        And(
          IsNull(UnresolvedAttribute("campaign_medium")),
          IsNull(UnresolvedAttribute("campaign_source"))
        ),
      CaseWhen(innerCaseBranchExprs.flatten :+ Literal("Other")),
      UnresolvedAttribute("campaign_medium")
    )
    Alias(CaseWhen(caseBranchesExpr), "sd_medium")()

    /*
    val df = sql (
    """
    select
    case when (campaign_medium is null and campaign_source is null ) then
                ( case
                    when ( sd_source is null and sd_medium is null and sd_host is null)  then 'Direct'
                    when ( sd_source is null and sd_medium is null and sd_host like '%google.com%') then 'Search'
                    when ( sd_source is null and sd_medium is null and sd_host like '%googleadservices%') then 'Adwords'
                    when ( sd_source is null and sd_medium is null and sd_host like '%cart%') then 'Cart'
                    when ( sd_source is null and sd_medium is null and sd_host like '%home-l->er-hair%') then 'Search'
                    when ( sd_source is null and sd_medium is null and sd_host like '%age-defying-l->er%') then 'Search'
                    when ( sd_source is null and sd_medium is null and sd_host like '%Quest%') then 'Questionnaire'
                    when ( sd_source is null and sd_medium is null and sd_host like '%orderReview%') then 'OrderReview'
                    when ( sd_source is null and sd_medium is null and sd_host like '%yahoo%') then 'Yahoo'
                    when ( sd_source is null and sd_medium is null and sd_host like '%bing%') then 'Bing'
                    when ( sd_source is null and sd_medium is null and sd_query like '%gclid%') then 'Adwords'
                     when ( sd_source is null and sd_medium is null and sd_host like '%googleadservices%') then 'Adwords'
                    else 'Other'
                    end )

            else
            campaign_medium
            end as sd_medium
    from tom
  """)
  */
  }


}
