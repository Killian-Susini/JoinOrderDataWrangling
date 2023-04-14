#include <chrono>
#include <iostream>
#include <tuple>
#include <vector>
#include <string>
#include <random>
#include <array>
#include <numeric>
#include <algorithm>
#include <bitset>
#include <chrono>
#include <cmath>
#include <omp.h>
#include <fstream>
#include <format>

using namespace std;
random_device rd;
using tableName = int;

array<uniform_int_distribution<>, 4> cardinalities_class = { uniform_int_distribution<>(10, 100),
                                                            uniform_int_distribution<>(100, 1000),
                                                            uniform_int_distribution<>(1000, 10000),
                                                            uniform_int_distribution<>(10000, 100000)};

discrete_distribution<> cardinalities_class_distribution({ 15, 30, 35, 20 });
array<uniform_int_distribution<>, 4> attribute_domain_class = { uniform_int_distribution<>(2, 10),
                                                            uniform_int_distribution<>(10, 100),
                                                            uniform_int_distribution<>(100, 500),
                                                            uniform_int_distribution<>(500, 1000) };
discrete_distribution<> attribute_domain_distribution({ 4, 50, 30, 15 });

struct Query
{
    vector<int> tablesCardinality;
    vector<tuple<tableName, tableName, double>> predicates;
};

Query generateRandomQuery(size_t numberOfTables, string graphClass) {
    default_random_engine generator(rd());
    vector<tableName> tableNames(numberOfTables);
    iota(tableNames.begin(), tableNames.end(), 0);
    shuffle(tableNames.begin(), tableNames.end(), generator);

    Query query{};
    for (size_t i = 0; i < numberOfTables; i++)
    {
        query.tablesCardinality.push_back(cardinalities_class[cardinalities_class_distribution(generator)](generator));
    }
    if (graphClass == "chain") {
        for (size_t j = 0; j < numberOfTables - 1; j++) {
            int domainAttribute1 = attribute_domain_class[attribute_domain_distribution(generator)](generator);
            int domainAttribute2 = attribute_domain_class[attribute_domain_distribution(generator)](generator);
            query.predicates.push_back(tuple<tableName, tableName, double>(tableNames[j], tableNames[j + 1], 1.0 / min(domainAttribute1, domainAttribute2)));
        }
    }
    else if (graphClass == "cycle") {
        for (size_t j = 0; j < numberOfTables - 1; j++) {
            int domainAttribute1 = attribute_domain_class[attribute_domain_distribution(generator)](generator);
            int domainAttribute2 = attribute_domain_class[attribute_domain_distribution(generator)](generator);
            query.predicates.push_back(tuple<tableName, tableName, double>(tableNames[j], tableNames[j + 1], 1.0 / min(domainAttribute1, domainAttribute2)));
        }
        int domainAttribute1 = attribute_domain_class[attribute_domain_distribution(generator)](generator);
        int domainAttribute2 = attribute_domain_class[attribute_domain_distribution(generator)](generator);
        query.predicates.push_back(tuple<tableName, tableName, double>(tableNames.back(), tableNames.front(), 1.0 / min(domainAttribute1, domainAttribute2)));
    }
    else if (graphClass == "star") {
        for (size_t j = 1; j < numberOfTables; j++) {
            int domainAttribute1 = attribute_domain_class[attribute_domain_distribution(generator)](generator);
            int domainAttribute2 = attribute_domain_class[attribute_domain_distribution(generator)](generator);
            query.predicates.push_back(tuple<tableName, tableName, double>(tableNames.front(), tableNames[j], 1.0 / min(domainAttribute1, domainAttribute2)));
        }
    }

    return query;
}

void createTestFiles() {
    ofstream file;
    for (auto type : { "chain", "cycle", "star" })
        for (size_t i = 10; i <= 60; i += 10)
        {
            for (size_t j = 0; j < 30; j++)
            {
                auto query = generateRandomQuery(i, type);
                file.open(format("testQuery\\{}_{}_{}.txt", type, i, j));
                for (auto card : query.tablesCardinality)
                    file << card << "\n";
                file << "Predicate" << "\n";
                for (auto predicate : query.predicates)
                    file << get<0>(predicate) << " " << get<1>(predicate) << " " << get<2>(predicate) << "\n";

                file.close();
            }
        }
}


struct State
{
    vector<double> tablesCardinality;
    vector<double> predicateSelectivity;
    vector<tableName> tableLeftToJoin;
    vector<int> predicatesLeftToApply;
    vector<int> moves; // may not be needed.
    vector<bitset<64>> tablesPresentInJoins; //will be enough, need to be more than the number of tables to join
    vector<bitset<64>> tablesNeededForPredicates; //will be enough, need to be more than the number of tables to join


    int firstTableSelected = -1;
    size_t initialNumberOfTables;
    size_t numberOfTables;
    //double currentScore;

    State(Query query) {
        for (size_t i = 0; i < query.tablesCardinality.size(); i++) {
            tablesCardinality.push_back(query.tablesCardinality[i]);
            tableLeftToJoin.push_back(i);
            tablesPresentInJoins.push_back(bitset<64>{});
            tablesPresentInJoins.back()[i] = true;
        }
        initialNumberOfTables = query.tablesCardinality.size();
        numberOfTables = query.tablesCardinality.size();
        for (size_t i = 0; i < query.predicates.size(); i++) {
            predicateSelectivity.push_back(get<2>(query.predicates[i]));
            predicatesLeftToApply.push_back(i);
            tablesNeededForPredicates.push_back(bitset<64>{});
            tablesNeededForPredicates.back()[get<0>(query.predicates[i])] = true;
            tablesNeededForPredicates.back()[get<1>(query.predicates[i])] = true;
        }
        firstTableSelected = -1;
        moves = {};
    }
};


inline bool terminal(const State& currentState) {
    return (currentState.numberOfTables == 2*currentState.initialNumberOfTables - 1);
}



inline double score(const State& currentState) {
    // we don't check if there are still tables left to join, since the cost can only go up,
    // we can use the results to stop a playout early.
    double computed_score = 0.;
    for (size_t i = currentState.initialNumberOfTables; i < currentState.tablesCardinality.size(); i++)// the error due to adding small and large double is negligeable here because of the limited number of additions.
        computed_score += currentState.tablesCardinality[i];
    return computed_score;
}


inline int sampleMove(const State& currentState, default_random_engine& generator, bool cartesian_allowed) {
    if (cartesian_allowed || currentState.firstTableSelected == -1) {
        uniform_int_distribution<> sampler(0, currentState.tableLeftToJoin.size() - 1);
        return sampler(generator);
    }
    else {
        // first sift predicates partially applicable to selected table,
        vector<bitset<64>> predicateToApply = {};
        for (auto predIndex: currentState.predicatesLeftToApply) {
            if ((currentState.tablesNeededForPredicates[predIndex] & currentState.tablesPresentInJoins[currentState.firstTableSelected]).any())
                predicateToApply.push_back(currentState.tablesNeededForPredicates[predIndex]);
        }
        // then sift tables left to be join for which one of the predicate is applicable
        vector<int> possibleToSelect = {};
        for (size_t i = 0; i < currentState.tableLeftToJoin.size(); i++) {
            for (auto pred : predicateToApply)
                if ((currentState.tablesPresentInJoins[currentState.tableLeftToJoin[i]] & pred).any()) {
                    possibleToSelect.push_back(i);
                    break;
                }
        }

        uniform_int_distribution<> sampler(0, possibleToSelect.size() - 1);
        return possibleToSelect[sampler(generator)];
    }
}

inline void playBushy(State& currentState, int moveIndex) {
    // cout << moveIndex << '\n';
    if (currentState.firstTableSelected == -1) {
        // remember selected table
        currentState.firstTableSelected = currentState.tableLeftToJoin[moveIndex];
        // remove elements
        // swap to back and pop
        currentState.tableLeftToJoin[moveIndex] = currentState.tableLeftToJoin.back();
        currentState.tableLeftToJoin.pop_back();
    }
    else {
        // compute new table log cardinality
        auto firstTable = currentState.firstTableSelected;
        auto secondTable = currentState.tableLeftToJoin[moveIndex];
        auto newTableCardinality = currentState.tablesCardinality[firstTable] * currentState.tablesCardinality[secondTable];
        currentState.tablesPresentInJoins.push_back(currentState.tablesPresentInJoins[firstTable] | currentState.tablesPresentInJoins[secondTable]);

        vector<size_t> toRemove{};
        for (size_t i = 0; i < currentState.predicatesLeftToApply.size(); i++) {
            if ((currentState.tablesPresentInJoins.back() & currentState.tablesNeededForPredicates[currentState.predicatesLeftToApply[i]]) == currentState.tablesNeededForPredicates[currentState.predicatesLeftToApply[i]]) { // predicate is applicable, here we can apply it directly at no cost
                newTableCardinality *= currentState.predicateSelectivity[currentState.predicatesLeftToApply[i]];
                toRemove.push_back(i);
            }
        } // most efficient?
        for (int index = toRemove.size() - 1; index >= 0; index--) { // need to do in reverse to avoid messing with indexes as we remove
            currentState.predicatesLeftToApply[toRemove[index]] = currentState.predicatesLeftToApply.back();
            currentState.predicatesLeftToApply.pop_back();
        }

        //currentState.joinPairs.push_back(tuple<tableName, tableName>(firstTable, secondTable)); 

        // remove element and add new table
        currentState.firstTableSelected = -1;
        currentState.tableLeftToJoin[moveIndex] = currentState.numberOfTables++; // remove secondTable by adding the new table, then increase numberOfTables
        currentState.tablesCardinality.push_back(newTableCardinality);
    }
    currentState.moves.push_back(moveIndex);
}


inline void playLeftDeep(State& currentState, int moveIndex) {
    if (currentState.firstTableSelected == -1) {
        // remember selected table
        currentState.firstTableSelected = currentState.tableLeftToJoin[moveIndex];
        // remove elements
        // swap to back and pop
        currentState.tableLeftToJoin[moveIndex] = currentState.tableLeftToJoin.back();
        currentState.tableLeftToJoin.pop_back();
    }
    else {
        // compute new table log cardinality
        auto firstTable = currentState.firstTableSelected;
        auto secondTable = currentState.tableLeftToJoin[moveIndex];
        auto newTableCardinality = currentState.tablesCardinality[firstTable] * currentState.tablesCardinality[secondTable];
        currentState.tablesPresentInJoins.push_back(currentState.tablesPresentInJoins[firstTable] | currentState.tablesPresentInJoins[secondTable]);

        vector<size_t> toRemove{};
        for (size_t i = 0; i < currentState.predicatesLeftToApply.size(); i++) {
            if ((currentState.tablesPresentInJoins.back() & currentState.tablesNeededForPredicates[currentState.predicatesLeftToApply[i]]) == currentState.tablesNeededForPredicates[currentState.predicatesLeftToApply[i]]) { // predicate is applicable, here we can apply it directly at no cost
                newTableCardinality *= currentState.predicateSelectivity[currentState.predicatesLeftToApply[i]];
                toRemove.push_back(i);
            }
        } // most efficient?
        for (int index = toRemove.size() - 1; index >= 0; index--) { // need to do in reverse to avoid messing with indexes as we remove
            currentState.predicatesLeftToApply[toRemove[index]] = currentState.predicatesLeftToApply.back();
            currentState.predicatesLeftToApply.pop_back();
        }

        //currentState.joinPairs.push_back(tuple<tableName, tableName>(firstTable, secondTable)); 

        currentState.tableLeftToJoin[moveIndex] = currentState.tableLeftToJoin.back();
        currentState.tableLeftToJoin.pop_back();

        currentState.firstTableSelected = currentState.numberOfTables++; // assign the new table to the first first selection
        currentState.tablesCardinality.push_back(newTableCardinality); // add its cardinality
    }
    currentState.moves.push_back(moveIndex);
}

/////return int instead
inline State playoutBushy(State currentState, default_random_engine& generator, bool cartesian_allowed) { /////return int instead
    while (!terminal(currentState)) {
        auto moveIndex = sampleMove(currentState, generator, cartesian_allowed);
        //cout << currentState.numberOfTables << " playout move = " << moveIndex << '\n';
        playBushy(currentState, moveIndex);
    }
    return currentState;
}
inline State playoutLeftDeep(State currentState, default_random_engine& generator, bool cartesian_allowed) { /////return int instead
    while (!terminal(currentState)) {
        auto moveIndex = sampleMove(currentState, generator, cartesian_allowed);
        //cout << moveIndex << '\n';
        playLeftDeep(currentState, moveIndex);
    }
    return currentState;
}

/////return int instead
State nestedBushy(State& state, int level, default_random_engine& generator, bool cartesian_allowed) { /////return int instead
    if (level == 0) {
        return playoutBushy(state, generator, cartesian_allowed);
    }
    State bestSoFar = state;
    double bestScore = DBL_MAX;
    while (!terminal(state)) {
        if (!cartesian_allowed && (state.firstTableSelected != -1)) {
            // first sift predicates partially applicable to selected table,
            vector<bitset<64>> predicateToApply = {};
            for (auto predIndex : state.predicatesLeftToApply) {
                if ((state.tablesNeededForPredicates[predIndex] & state.tablesPresentInJoins[state.firstTableSelected]).any())
                    predicateToApply.push_back(state.tablesNeededForPredicates[predIndex]);
            }
            // then sift tables left to be join for which one of the predicate is applicable
            vector<int> possibleToSelect = {};
            for (size_t i = 0; i < state.tableLeftToJoin.size(); i++) {
                for (auto pred : predicateToApply)
                    if ((state.tablesPresentInJoins[state.tableLeftToJoin[i]] & pred).any()) {
                        possibleToSelect.push_back(i);
                        break;
                    }
            }
            for (auto move : possibleToSelect) {
                State copy_state = state;
                // play move
                playBushy(copy_state, move);
                copy_state = nestedBushy(copy_state, level - 1, generator, cartesian_allowed);
                int newScore = score(copy_state);
                if (newScore < bestScore)
                {
                    bestScore = newScore;
                    bestSoFar = copy_state;
                }
            }

        }
        else {
            for (size_t move = 0; move < state.tableLeftToJoin.size(); move++) {
                State copy_state = state;
                // play move
                playBushy(copy_state, move);
                copy_state = nestedBushy(copy_state, level - 1, generator, cartesian_allowed);
                int newScore = score(copy_state);
                if (newScore < bestScore)
                {
                    bestScore = newScore;
                    bestSoFar = copy_state;
                }
            }
        }
        // play best move found, that is the move that the best sequence follows here.
        size_t move = bestSoFar.moves[state.moves.size()];
        //cout << "nested move " << move << '\n';
        playBushy(state, move);

    }
    return bestSoFar;
}

State nestedLeftDeep(State& state, int level, default_random_engine& generator, bool cartesian_allowed) { /////return int instead
    if (level == 0) {
        return playoutLeftDeep(state, generator, cartesian_allowed);
    }
    State bestSoFar = state;
    double bestScore = DBL_MAX;
    while (!terminal(state)) {
        if (!cartesian_allowed && (state.firstTableSelected != -1)) {
            // first sift predicates partially applicable to selected table,
            vector<bitset<64>> predicateToApply = {};
            for (auto predIndex : state.predicatesLeftToApply) {
                if ((state.tablesNeededForPredicates[predIndex] & state.tablesPresentInJoins[state.firstTableSelected]).any())
                    predicateToApply.push_back(state.tablesNeededForPredicates[predIndex]);
            }
            // then sift tables left to be join for which one of the predicate is applicable
            vector<int> possibleToSelect = {};
            for (size_t i = 0; i < state.tableLeftToJoin.size(); i++) {
                for (auto pred : predicateToApply)
                    if ((state.tablesPresentInJoins[state.tableLeftToJoin[i]] & pred).any()) {
                        possibleToSelect.push_back(i);
                        break;
                    }
            }
            for (auto move: possibleToSelect) {
                State copy_state = state;
                // play move
                playLeftDeep(copy_state, move);
                copy_state = nestedLeftDeep(copy_state, level - 1, generator, cartesian_allowed);
                int newScore = score(copy_state);
                if (newScore < bestScore)
                {
                    bestScore = newScore;
                    bestSoFar = copy_state;
                }
            }
            
        }
        else {
            for (size_t move = 0; move < state.tableLeftToJoin.size(); move++) {
                State copy_state = state;
                // play move
                playLeftDeep(copy_state, move);
                copy_state = nestedLeftDeep(copy_state, level - 1, generator, cartesian_allowed);
                int newScore = score(copy_state);
                if (newScore < bestScore)
                {
                    bestScore = newScore;
                    bestSoFar = copy_state;
                }
            }
        }
        // play best move found, that is the move that the best sequence follows here.
        size_t move = bestSoFar.moves[state.moves.size()];
        playLeftDeep(state, move);

    }
    return bestSoFar;
}

/////return int instead
State outerNestedBushy(State& state, int level, bool cartesian_allowed, bool verbose = false) { /////return int instead
    default_random_engine generator(rd());
    if (level == 0) {
        return playoutBushy(state, generator, cartesian_allowed);
    }
    State bestSoFar = state;
    double bestScore = DBL_MAX;
    while (!terminal(state)) {
        if (!cartesian_allowed && (state.firstTableSelected != -1)) {
            // first sift predicates partially applicable to selected table,
            vector<bitset<64>> predicateToApply = {};
            for (auto predIndex : state.predicatesLeftToApply) {
                if ((state.tablesNeededForPredicates[predIndex] & state.tablesPresentInJoins[state.firstTableSelected]).any())
                    predicateToApply.push_back(state.tablesNeededForPredicates[predIndex]);
            }
            // then sift tables left to be join for which one of the predicate is applicable
            vector<int> possibleToSelect = {};
            for (size_t i = 0; i < state.tableLeftToJoin.size(); i++) {
                for (auto pred : predicateToApply)
                    if ((state.tablesPresentInJoins[state.tableLeftToJoin[i]] & pred).any()) {
                        possibleToSelect.push_back(i);
                        break;
                    }
            }
            for (auto move : possibleToSelect) {
                State copy_state = state;
                // play move
                playBushy(copy_state, move);
                copy_state = nestedBushy(copy_state, level - 1, generator, cartesian_allowed);
                int newScore = score(copy_state);
                if (newScore < bestScore)
                {
                    bestScore = newScore;
                    bestSoFar = copy_state;
                }
            }

        }
        else {
            for (size_t move = 0; move < state.tableLeftToJoin.size(); move++) {
                State copy_state = state;
                // play move
                playBushy(copy_state, move);
                copy_state = nestedBushy(copy_state, level - 1, generator, cartesian_allowed);
                int newScore = score(copy_state);
                if (newScore < bestScore)
                {
                    bestScore = newScore;
                    bestSoFar = copy_state;
                }
            }
        }
        // play best move found, that is the move that the best sequence follows here.
        size_t move = bestSoFar.moves[state.moves.size()];
        if (verbose) cout << state.tableLeftToJoin[move] << '\n';

        playBushy(state, move);

    }
    return bestSoFar;
}

State outerNestedLeftDeep(State& state, double bestScore, int level, bool cartesian_allowed, bool verbose = false) { /////return int instead
    default_random_engine generator(rd());
    if (level == 0) {
        return playoutLeftDeep(state, generator, cartesian_allowed);
    }
    State bestSoFar = state;
    //double bestScore = DBL_MAX;
    while (!terminal(state)) {
        if (!cartesian_allowed && (state.firstTableSelected != -1)) {
            // first sift predicates partially applicable to selected table,
            vector<bitset<64>> predicateToApply = {};
            for (auto predIndex : state.predicatesLeftToApply) {
                if ((state.tablesNeededForPredicates[predIndex] & state.tablesPresentInJoins[state.firstTableSelected]).any())
                    predicateToApply.push_back(state.tablesNeededForPredicates[predIndex]);
            }
            // then sift tables left to be join for which one of the predicate is applicable
            vector<int> possibleToSelect = {};
            for (size_t i = 0; i < state.tableLeftToJoin.size(); i++) {
                for (auto pred : predicateToApply)
                    if ((state.tablesPresentInJoins[state.tableLeftToJoin[i]] & pred).any()) {
                        possibleToSelect.push_back(i);
                        break;
                    }
            }
            for (auto move : possibleToSelect) {
                State copy_state = state;
                // play move
                playLeftDeep(copy_state, move);
                copy_state = nestedLeftDeep(copy_state, level - 1, generator, cartesian_allowed);
                int newScore = score(copy_state);
                if (newScore < bestScore)
                {
                    bestScore = newScore;
                    bestSoFar = copy_state;
                }
            }

        }
        else {
            for (size_t move = 0; move < state.tableLeftToJoin.size(); move++) {
                State copy_state = state;
                // play move
                playLeftDeep(copy_state, move);
                copy_state = nestedLeftDeep(copy_state, level - 1, generator, cartesian_allowed);
                int newScore = score(copy_state);
                if (newScore < bestScore)
                {
                    bestScore = newScore;
                    bestSoFar = copy_state;
                }
            }
        }
        // play best move found, that is the move that the best sequence follows here.
        size_t move = bestSoFar.moves[state.moves.size()];
        if (verbose) cout << state.tableLeftToJoin[move] << '\n';
        playLeftDeep(state, move);

    }
    return bestSoFar;
}

// need multiple generators, right now it is wrong! (and slow!)
State parallelOuterNestedBushy(State& state, double bestScore, int level, bool cartesian_allowed=true) {
    default_random_engine generator(rd());
    if (level == 0) {
        return playoutBushy(state,  generator, cartesian_allowed);
    }
    State bestSoFar = state;
    State bestSoFarPrivate = state;
    double bestScorePrivate = bestScore;
    #pragma omp parallel
    while (!terminal(state)) {
        #pragma omp for private(bestScorePrivate, bestSoFarPrivate) nowait
        for (size_t move = 0; move < state.tableLeftToJoin.size(); move++) {
            State copy_state = state;
            // play move
            playBushy(copy_state, move);
            copy_state = nestedBushy(copy_state, level - 1, generator, cartesian_allowed);
            int newScore = score(copy_state);
            if (newScore < bestScorePrivate)
            {
                bestScorePrivate = newScore;
                bestSoFarPrivate = copy_state;
            }
        }
        #pragma omp critical
        if (bestScorePrivate < bestScore) // maybe instead use only the master thread to set bestScore and bestScorePrivate
        {
            bestScore = bestScorePrivate;
            bestSoFar = bestSoFarPrivate;
        }
        #pragma omp barrier
        {
            bestScorePrivate = bestScore;
            bestSoFarPrivate = bestSoFar;
        }
        // play best move found, that is the move that the best sequence follows here.
        #pragma omp master
        {
            size_t move = bestSoFar.moves[state.moves.size()];
            //cout << state.tableLeftToJoin[move] << '\n';
            playBushy(state, move);
        }
    }
    return bestSoFar;
}

void testOnFiles(bool bushy_allowed, bool cartesian_allowed) {
    using std::chrono::high_resolution_clock;
    using std::chrono::duration_cast;
    using std::chrono::duration;
    using std::chrono::milliseconds;
    using std::chrono::microseconds;
    string line;
    ifstream file;
    ofstream outfile;
    string saveDirectory = "";
    if (bushy_allowed && cartesian_allowed)
        saveDirectory = "resultsNMCS\\";
    else if (bushy_allowed && !cartesian_allowed)
        saveDirectory = "resultsNMCS_NoCartesian\\";
    else if (!bushy_allowed && cartesian_allowed)
        saveDirectory = "resultsNMCS_NoBushy\\";
    else if (!bushy_allowed && !cartesian_allowed)
        saveDirectory = "resultsNMCS_NoCartesian_NoBushy\\";

    for (size_t nest = 0; nest < 4; nest++) {
        for (size_t i = 10; i <= 10; i += 10)
        {

            for (auto type : { "chain", "cycle", "star" })
                for (size_t j = 0; j < 1; j++)
                {
                    bool predicate_seen = false;
                    Query query{};
                    file.open(format("testQuery\\{}_{}_{}.txt", type, i, j));
                    if (!file.is_open()) {
                        perror("Error open");
                        exit(EXIT_FAILURE);
                    }
                    while (getline(file, line)) {
                        if (!predicate_seen) {
                            if (line == "Predicate") {
                                predicate_seen = true;
                                continue;
                            }
                            int tableCardinality;
                            sscanf_s(line.c_str(), "%d", &tableCardinality);
                            query.tablesCardinality.push_back(tableCardinality);
                        }
                        else {
                            int table1, table2;
                            double selectivity;
                            sscanf_s(line.c_str(), "%d %d %lf", &table1, &table2, &selectivity);
                            query.predicates.push_back(tuple<int, int, double>(table1, table2, selectivity));
                        }
                    }
                    file.close();

                    outfile.open(format("{}distOfNest{}_res_{}_{}_{}.txt", saveDirectory, nest, type, i, j));
                    if (!outfile.is_open()) {
                        perror("Error open");
                        exit(EXIT_FAILURE);
                    }
                    for (size_t k = 0; k < 100; k++)
                    {
                        State startState(query);
                        State endState(query);
                        auto t1 = high_resolution_clock::now();

                        if (bushy_allowed)
                            endState = outerNestedBushy(startState, DBL_MAX, nest, cartesian_allowed);
                        else
                            endState = outerNestedLeftDeep(startState, DBL_MAX, nest, cartesian_allowed);
                        auto t2 = high_resolution_clock::now();
                        auto ms_int = duration_cast<milliseconds>(t2 - t1);

                        //write test results
                        outfile << score(endState) << " " << ms_int.count() << "ms " << "\n";
                    }
                    outfile.close();
                    std::cout << format("{}distOfNest{}_res_{}_{}_{}.txt", saveDirectory, nest, type, i, j) << '\n';
                }
        }
    }
}

int main()
{
    using std::chrono::high_resolution_clock;
    using std::chrono::duration_cast;
    using std::chrono::duration;
    using std::chrono::milliseconds;
    using std::chrono::microseconds;
    /*
    auto query = generateRandomQuery(25, "star");
    cout << "----------TABLES----------\n";
    for (size_t i = 0; i < query.tablesCardinality.size(); i++) {
        cout << "Table " << i << ": " << query.tablesCardinality[i] << "\n";
    }
    cout << "--------PREDICATES--------\n";
    for (auto& p : query.predicates) {
        cout << "Predicate on (" << get<0>(p) << ", " << get<1>(p) << "): " << get<2>(p) << "\n";
    }
    default_random_engine generator(rd());
    auto t1 = high_resolution_clock::now();
    size_t n = 150;
    for (size_t i = 0; i < n; i++)
    {
        query = generateRandomQuery(25, "chain");
        State startState(query);
        auto endState = playout(startState, DBL_MAX, generator);
    }
    auto t2 = high_resolution_clock::now();
    auto ms_int = duration_cast<milliseconds>(t2 - t1);
    cout << "time spent playing " << n << " games: " << ms_int.count() << "ms\n";
    */
    /*auto query = generateRandomQuery(10, "chain");
    auto t1 = high_resolution_clock::now();
    for (size_t i = 0; i < 10; i++)
    {
        query = generateRandomQuery(10, "chain");
        State startState(query);
        State endState = outerNestedLeftDeep(startState, DBL_MAX, 3, false, false);
        //cout << i << '\n';
    }
    auto t2 = high_resolution_clock::now();
    auto ms_int = duration_cast<milliseconds>(t2 - t1);
    std::cout << "time spent playing optimising " << 10 << " games: " << ms_int.count() << "ms\n";
    */
    //createTestFiles();
    testOnFiles(false, false);
    testOnFiles(false, true);
    testOnFiles(true, false);
    testOnFiles(true, true);
    return 0;
}
