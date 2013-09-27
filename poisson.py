import matplotlib
matplotlib.use('Agg')





import math
import matplotlib.pyplot as plt

def poisson(m):
    def f(k):
        e = math.e**(-m)
        f = math.factorial(k)
        g = m**k
        return g*e/f
    return f

R = range(20)
L = list()
means = (1,2,4,10)
for m in means:
    f = poisson(m)
    L.append([f(k) for k in R])
colors = ['r','g','b','purple']

for c,P in zip(colors,L):
    plt.plot(R,P,zorder=1,color='0.2',lw=1.5)
    plt.scatter(R,P,zorder=2,s=150,color=c)
    
ax = plt.axes()
ax.set_xlim(-0.5,20)
ax.set_ylim(-0.01,0.4)
plt.savefig('/root/ts/timeseries/timeseries/static/images/poisson.png')
