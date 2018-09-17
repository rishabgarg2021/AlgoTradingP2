import numpy as np
# need to create list which has current amount we hold of each asset [A, B, C, N]
# needs to be updated every time we interact with the market
current_holdings = [14, 1, 0, 15]
cash = 0
#need to create 4 lists which display the payoff's in states [W, X, Y, Z] <- grab from dictionary _payoffs
a_payoff = [1000, 0, 750, 250]
b_payoff = [0, 250, 750, 1000]
c_payoff = [0, 750, 250, 1000]
d_payoff = [500, 500, 500, 500]

penalty_risk = 0.01

# creates a matrix covariance which we will use to calculate portfolio variance
abcd_cov = np.cov(np.vstack([a_payoff,b_payoff,c_payoff,d_payoff]),ddof=0)

# converts the asset holdings list into an array of size number of [assets * 1]
weights = np.array([[x] for x in current_holdings])
print(weights)

#cacluates the payoff variance using matrix formula of [weights'*Covariance Matrix * weights]
payoff_variance = np.dot(weights.T, np.dot(abcd_cov, weights))

#calcualtes the expected payoff as a array multiplication of [asset holdings * expected payoff for each asset]
expected_payoff = np.sum((weights*np.average(np.vstack([a_payoff,b_payoff,c_payoff,d_payoff]))))

#calculates performance using the formula outline where [Performnace = cash + expected payoff - risk_penalty (b) * payoff variance]
performance = cash+expected_payoff-payoff_variance*penalty_risk

#Printing the results to test / check

print("ARRAY WITH POSSIBLE STATES WXYZ's PAYOFFS")
print(np.vstack([a_payoff, b_payoff, c_payoff, d_payoff]))

print("WEIGHTS - CURRENT HOLDING OF EACH ASSET [A, B, C, N]")
print(weights)

print("COVARIANCE MATRIX")
print(abcd_cov)

print("COMPUTED PAYOFF VARIANCE")
print(payoff_variance[0][0])

print("EXPECTED PAYOFF")
print(expected_payoff)

print("PERFORMANCE:")
print(performance[0][0])
