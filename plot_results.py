import matplotlib.pyplot as plt
import numpy as np
import os
import math
if __name__ == "__main__":
    dir_path = "C:\\Users\\killi\\source\\repos\\JoinOrderMonteCarlo\\"
    plot_dict = dict()
    for graph_type in ["chain", "cycle", "star"]:
        for nest in [2,4]:
            for i in range(10,11,10):
                for j in range(1):
                    model_file = f"distOfNest{nest}_res_{graph_type}_{i}_{j}.txt"
                    scores = []
                    times = []
                    with open(os.path.join(dir_path,"resultsNMCS\\",model_file), "r") as res_file:
                        for line in res_file.readlines():
                            score, time, _ = line.split(" ")
                            scores.append(math.log10(float(score)))
                            times.append(int(time[:-2]))
                            print(score, time)
                    plt.hist(scores,bins=100, label=f'nest={nest} graph_type={graph_type}')
        plt.legend()
        plt.savefig(os.path.join(dir_path, f"plot_{graph_type}_compare_nested_2_and_4"))
        plt.clf()