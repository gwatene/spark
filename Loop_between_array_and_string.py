def solution(S, A):
    # write your code in Python 3.6
    res = S[0:1]
    #for i in range(A.length):
    i=0
    k=0
    while(True):
        res+=S[A[k]:A[k]+1]
        k=A[k]
        if A[A[k]]==0:
           res+=S[A[k]:A[k]+1]
           break
    print(res)
    return res 
