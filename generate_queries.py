import random
import enum

random.seed()
graph_classes=["chain", "cycle", "star"]
cardinalities_class = [(10,100),(100,1_000),(1_000,10_000),(10_000,100_000)]
cardinalities_class_probability = [15, 30, 35, 20]
attribute_domain_class = [(2,10),(10,100),(100,500),(500,1000)]
attribute_domain_class_probability = [5,50,30,15]

def generate_random_query(number_of_tables : int, graph_class : str):
    assert(graph_class in graph_classes)
    tables_cardinality_class = random.choices(cardinalities_class, weights=cardinalities_class_probability, k=number_of_tables)
    tables_cardinalities = list(enumerate([random.randrange(start, stop) for start,stop in tables_cardinality_class]))
    join_predicates = []
    match graph_class:
        case "chain":
            order = list(range(number_of_tables))
            random.shuffle(order)
            att1_min_max = random.choices(attribute_domain_class,weights=attribute_domain_class_probability,k=len(order)-1)
            att2_min_max = random.choices(attribute_domain_class,weights=attribute_domain_class_probability,k=len(order)-1)
            att1_domains = [random.randrange(start, stop) if start!='cartesian' else 'cartesian' for start,stop in att1_min_max ]
            att2_domains = [random.randrange(start, stop) if start!='cartesian' else 'cartesian' for start,stop in att2_min_max]
            join_predicates = [(order[i],order[i+1],1/min(att1_domains[i], att2_domains[i])) for i in range(len(order)-1)]
        case "cycle":
            order = list(range(number_of_tables))
            random.shuffle(order)
            att1_min_max = random.choices(attribute_domain_class,weights=attribute_domain_class_probability,k=len(order))
            att2_min_max = random.choices(attribute_domain_class,weights=attribute_domain_class_probability,k=len(order))
            att1_domains = [random.randrange(start, stop) if start!='cartesian' else 'cartesian' for start,stop in att1_min_max]
            att2_domains = [random.randrange(start, stop) if start!='cartesian' else 'cartesian' for start,stop in att2_min_max]
            join_predicates = [(order[i],order[i+1],1/min(att1_domains[i], att2_domains[i])) for i in range(len(order)-1)]
            join_predicates.append((order[-1], order[0], 1/min(att1_domains[-1],att2_domains[-1],)))
        case "star":
            order = list(range(number_of_tables))
            central = random.choice(order)
            order = list(filter(lambda x: x!=central,order))
            att1_min_max = random.choices(attribute_domain_class,weights=attribute_domain_class_probability,k=len(order))
            att2_min_max = random.choices(attribute_domain_class,weights=attribute_domain_class_probability,k=len(order))
            att1_domains = [random.randrange(start, stop) if start!='cartesian' else 'cartesian' for start,stop in att1_min_max]
            att2_domains = [random.randrange(start, stop) if start!='cartesian' else 'cartesian' for start,stop in att2_min_max]
            join_predicates = [(central,order[i],1/min(att1_domains[i], att2_domains[i])) for i in range(len(order))]
            
            
        
    
    return tables_cardinalities,join_predicates
    
if __name__ == "__main__":
    tables_cardinalities,join_predicates = generate_random_query(10, "cycle")
    print((tables_cardinalities), (join_predicates))
    print(len(tables_cardinalities), len(join_predicates))
