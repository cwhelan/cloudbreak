# simulate data points at a locus with no deletion, with normal distribution for inserts, and noise points
# uniformly distributed from 0 to max.insert
sim.nodel <- function(coverage, insert.mean=200, insert.sd=30, max.insert=25000, num.noise=5) {
    num.true.reads <- rpois(1, coverage)
    true.inserts <- rnorm(num.true.reads, insert.mean, insert.sd)
    noise.inserts <- runif(num.noise, insert.mean, max.insert)
    c(true.inserts, noise.inserts)
}

# simulate data points at a locus with a homozygous deletion, with normal distribution for inserts, and noise points
# uniformly distributed from 0 to max.insert
sim.homdel <- function(coverage, del.size, insert.mean=200, insert.sd=30, max.insert=25000, num.noise=5) {
  num.true.reads <- rpois(1, coverage)
  true.inserts <- rnorm(num.true.reads, del.size + insert.mean, insert.sd)
  noise.inserts <- runif(num.noise, insert.mean, max.insert)
  c(true.inserts, noise.inserts)
}

# simulate data points at a locus with a heterozygous deletion, with normal distributions for inserts, and noise points
# uniformly distributed from 0 to max.insert
sim.hetdel <- function(coverage, del.size, insert.mean=200, insert.sd=30, max.insert=25000, num.noise=5) {
  num.true.reads <- rpois(1, coverage)
  nodel.inserts <- rnorm(num.true.reads / 2, insert.mean, insert.sd)
  del.inserts <- rnorm(num.true.reads / 2, del.size + insert.mean, insert.sd)
  noise.inserts <- runif(num.noise, insert.mean, max.insert)
  c(nodel.inserts, del.inserts, noise.inserts)
}

# compute log(p(y)) where y is N(mu, sigma)
mylognorm <- function(y, mu, sigma) {
  -1 * log(sigma + sqrt(2 * pi)) + -1 / 2 * ((y - mu) / sigma) ^ 2
}

# compute log likelihood y given the mixture
likelihood <- function(y, alpha, mu, sigma) {
  m1.likelihoods <- mylognorm(y, mu[1], sigma)
  m2.likelihoods <- mylognorm(y, mu[2], sigma)
  weighted.likelihoods <- logsumexp(c(alpha[1] +  m1.likelihoods, alpha[2] + m2.likelihoods))
  mean(weighted.likelihoods)
}

# logsumexp trick for precision
logsumexp <- function(x) {
  m <- max(x)
  s <- m + log(sum(exp(x - m)))
  s
}

# compute the gammas for the E step
gamma <- function(y, alpha, mu, sigma) {
  m1.likelihood <- alpha[1] + dnorm(y, mu[1], sigma, log=TRUE)
  m2.likelihood <- alpha[2] + dnorm(y, mu[2], sigma, log=TRUE)
  total.likelihood <- apply(cbind(m1.likelihood, m2.likelihood), 1, logsumexp)
  gamma1 <- m1.likelihood - total.likelihood
  gamma2 <- m2.likelihood - total.likelihood
  cbind(gamma1,gamma2)
}

# compute the n's for the E step
n.calc <- function(gamma.m) {
  c(logsumexp(gamma.m[,1]), logsumexp(gamma.m[,2]))
}

# compute alpha for the M step
alpha.update <- function(n, y) {
  n - log(length(y))
}

# update mu[2] (mu[1] is fixed) for the M step
mu2.update <- function(gamma, y, n) {
  exp(logsumexp(gamma[,2] + log(y)) - n[2])
}

# take an EM step
em.step <- function(y, alpha, mu, sigma) {
  gamma.m <- gamma(y, alpha, mu, sigma)
  n.m <- n.calc(gamma.m)
  alpha.1 <- alpha.update(n.m, y)  
  mu2.1 <- mu2.update(gamma.m, y, n.m)
  list(alpha=alpha.1, mu2=mu2.1)
}

# clean based on nearest neighbors, iterate EM, return updated alphas
em.alpha <- function(y, alpha, mu, sigma, max.iter=10) {
  mu1 <- mu[1]
  y <- y[mynnclean(y, sigma)]
  if(length(y) == 0) {
    return(log(c(1,0)))
  }
  l <- likelihood(y, alpha, mu, sigma)
  i <- 1
  repeat {  
    updates <- em.step(y,alpha,mu,sigma)
    alpha <- updates$alpha
    mu <- c(mu1, updates$mu2)
    l.prime <- likelihood(y, alpha, mu, sigma)  
    i <- i+1
    if (abs(l.prime - l) < 0.00001 | i > max.iter) {
      break
    }
  l <- l.prime
  }
  alpha
}

# remove points whose k'th nearest neighbor more than 5 sd's away
mynnclean <- function(x, sd, k=2) {
  d <- as.matrix(dist(x))
  ns = vector(mode="numeric", length=length(x))
  for (i in seq(1,length(x))) {
    ns[i] <- sort(d[i,])[k+1]
  }
  return(ns < 5 * sd)
}

# compute random samples of nodel, homozygous del, and heterozygous del, estimate alpha for each, and
# evaluate the number of times alpha is in the right range to call the genotype
testHomegrown <- function() {
  alpha <- log(c(.5,.5))
  mu <- c(200, 1000)
  sigma <- 30
  coverage <- 10
  tests <- 100
  noise <- 10
  delsize <- 300
  
  ws = vector(mode="numeric", length=tests)
  for (i in 1:tests) {
    #print(i)
    y <- sim.hetdel(coverage,delsize,num.noise=noise)
    #print(y)
    ws[i] <- em.alpha(y, alpha, mu, sigma)[1]
    #print(ws[i])
  }
  print(exp(ws))
  print(sum(exp(ws) > .25 & exp(ws) < .75))
  
  
  ws = vector(mode="numeric", length=tests)
  for (i in 1:tests) {
    #print(i)
    y <- sim.nodel(coverage, num.noise=noise)
    #print(y)
    ws[i] <- em.alpha(y, alpha, mu, sigma)[1]
    #print(ws[i])
  }
  print(exp(ws))
  print(sum(exp(ws) > .75))
  
  ws = vector(mode="numeric", length=tests)
  for (i in 1:tests) {
    #print(i)
    y <- sim.homdel(coverage,delsize, num.noise=noise)
    #print(y)
    ws[i] <- em.alpha(y, alpha, mu, sigma)[1]
    #print(ws[i])
  }
  print(exp(ws))
  print(sum(exp(ws) < .25))
}