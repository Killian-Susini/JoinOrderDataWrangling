import gurobipy as gp
import math
from generate_queries import generate_random_query
from gurobipy import GRB
import time
import func_timeout


def get_test_from_file(file):
    tables = []
    predicates = []
    predicate_seen = False
    table_num = 0
    with open(file, "r") as f:
      for line in f.readlines():
        if not predicate_seen:
          if line == "Predicate\n":
            predicate_seen = True
            continue
          tables.append((table_num,int(line)))
          table_num += 1
        else:
          t1, t2, select = line.split()
          predicates.append((int(t1), int(t2), float(select)))
          
    return tables, predicates
    


def solve(tables, predicates, precision,num_precision, savefile=None):
    # Create a new model
    m = gp.Model("Optimal Join Order")
    m.Params.TimeLimit = 90
    m.setParam(GRB.Param.NumericFocus, 3)
    
    BIG_M = 1000
    thresholds = [precision**i for i in range(num_precision)] # 10000,100000,1000000
    
    
    tables_name = [t[0] for t in tables]
    cardinalities = {t[0]:t[1] for t in tables}
    log_cardinalities = {t[0]:math.log10(t[1]) for t in tables}
    #•print("log card", log_cardinalities)
    
    j = list(range(len(tables)-1)) # 2 joins necessary to join everything
    
    p_T1 = [p[0] for p in predicates]
    p_T2 = [p[1] for p in predicates]
    selectivity = [p[2] for p in predicates]
    log_selectivity = [math.log10(p[2]) for p in predicates]
    
    log_thresholds = {r:math.log10(r) for r in thresholds}
    delta_thresholds = {thresholds[r]:thresholds[r] - thresholds[r-1] for r in range(1,len(thresholds))}
    delta_thresholds[thresholds[0]] = thresholds[0]
    # Create variables
    tio = m.addVars(tables_name,j, vtype=GRB.BINARY, name='tio')
    tii = m.addVars(tables_name,j, vtype=GRB.BINARY, name='tii')
    pao = m.addVars(((p_i,j_) for p_i in range(len(predicates)) for j_ in j), vtype=GRB.BINARY, name='pao')
    
    lco = m.addVars(j, name='lco')
    
    
    cto = m.addVars(thresholds,j, vtype=GRB.BINARY, name='cto')
    
    co = m.addVars(j, lb=0, name='co')
    ci = m.addVars(j, lb=0, name='ci')
    
    # Set objective: minimize approximate cardinalities
    m.setObjective(sum((co[j_] for j_ in j[1:])), GRB.MINIMIZE)

    m.addConstr(sum((tio[t, 0] for t in tables_name)) == 1, 
                name='initial outer table constraint')
    m.addConstrs((sum((tii[t, j_] for t in tables_name)) == 1 
                  for j_ in j), 
                name='inner table constraint')
    m.addConstrs((tio[t, j_] + tii[t, j_] <= 1 
                  for t in tables_name
                  for j_ in j), 
                name='overlaps table joins constraint')
    m.addConstrs((tio[t, j_] == tio[t, j_-1] + tii[t, j_-1] 
                  for t in tables_name 
                  for j_ in j[1:]), 
                name='outer table constraint')
    
    m.addConstrs((pao[p_i, j_] <= tio[p_T1[p_i], j_]
                  for p_i in range(len(predicates)) 
                  for j_ in j), 
                name='predicate constraint on first table')
    m.addConstrs((pao[p_i, j_] <= tio[p_T2[p_i], j_]
                  for p_i in range(len(predicates)) 
                  for j_ in j), 
                 name='predicate constraint on second table')
    
    m.addConstrs((ci[j_] == sum((tii[t, j_]*cardinalities[t] for t in tables_name)) for j_ in j), 
                name='Determines cardinality of inner operand')
    m.addConstrs((lco[j_] == sum((tio[t, j_]*log_cardinalities[t] for t in tables_name))
                             + sum((pao[p_i, j_]*log_selectivity[p_i] for p_i in range(len(predicates))))
                  for j_ in j), 
                name='Determines logarithm of outer operand cardinality')
    m.addConstrs((lco[j_] - BIG_M*cto[r, j_] <= log_thresholds[r] 
                  for r in thresholds
                  for j_ in j), 
                name='Threshold flag activation')
    m.addConstrs((co[j_] == sum((cto[r,j_]*(delta_thresholds[r]))
                                for r in thresholds) 
                  for j_ in j),
                name='Translate threshold Flag into approximate cardinality')
    
    m.optimize()
    
    if m.Status == GRB.INF_OR_UNBD:
        # Turn presolve off to determine whether model is infeasible
        # or unbounded
        #m.setParam(GRB.Param.Presolve, 0)
        m.setParam(GRB.Param.Presolve, 0)
        m.setParam(GRB.Param.NumericFocus, 3)
        m.optimize()

    if savefile != None:
      with open(savefile, "w") as sf:
        if m.Status == GRB.OPTIMAL:
            print(f"{sum((co[j_].X for j_ in j[1:])):e} {int(math.floor(m.Runtime*1000))}ms", file=sf)
        elif m.Status != GRB.INFEASIBLE:
            print('Optimization was stopped with status %d' % m.Status, file=sf)
        else:
            print("model is infeasible", file=sf)
        
        
    

if __name__ == "__main__":
    dir_path = "C:\\Users\\killi\\source\\repos\\JoinOrderMonteCarlo\\"
    #solve(*get_test_from_file(dir_path+"testQuery\\chain_20_10.txt"), 100, 30, dir_path +"resultsMILP\\chain_20_10.txt")
    num_precision = 60
    precision = 3
    for i in range(10,21,10):
      num_precision += 10
      for graph_type in ["chain", "cycle", "star"]:
        for j in range(1):
          model_file = f"{graph_type}_{i}_{j}.txt"
          try:
              func_timeout.func_timeout(
                  120, solve, args=[*get_test_from_file(dir_path+"testQuery\\" + model_file), precision, num_precision, dir_path +"resultsMILP\\" + model_file]
                  )
          except func_timeout.FunctionTimedOut:
            with open(dir_path +"resultsMILP\\" + model_file, "w") as sf:
              print('error for file', file=sf)
              continue
          
      