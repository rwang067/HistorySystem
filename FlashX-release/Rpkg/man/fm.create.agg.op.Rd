% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/FlashR.R
\name{fm.create.agg.op}
\alias{fm.create.agg.op}
\title{Create an aggregate operator}
\usage{
fm.create.agg.op(agg, combine, name)
}
\arguments{
\item{agg}{a \code{fm.basic.op} operator that computes partial aggregation
results.}

\item{combine}{a \code{fm.basic.op} operator that computes the final result.}

\item{name}{a string indicating the name of the aggregation operator.}
}
\value{
a \code{fm.agg.op} operator.
}
\description{
This function creates an aggregate operator for aggregation operations
on a FlashR object.
}
\details{
An Aggregate operator has two parts. \code{agg} computes partial
aggregation results and \code{combine} combines the partial aggregation
results to compute the final result. Both \code{agg} and \code{combine}
are the type of \code{fm.basic.op}.

The main reason of using two operators is for parallelization. Each thread
computes aggregation on part of the object. Eventually, we need an operator
to combine all partial aggregation results.

For many aggregation operations, \code{agg} and \code{combine} are the same.
For example, in the case of summation, both \code{agg} and \code{combine}
are simply \code{+}. In some cases, these two operators can be different.
For example, when counting the occurences of unique values, \code{agg}
is \code{count} and \code{combine} is \code{+}.
}
\examples{
agg.op <- fm.create.agg.op(fm.bo.add, fm.bo.add, "sum")
}

