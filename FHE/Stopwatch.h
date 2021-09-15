#ifndef STOPWATCH_H
#define STOPWATCH_H

#include <chrono>

using namespace std;

class Stopwatch
{
public:
    Stopwatch(string timer_name) :
            name_(timer_name),
            start_time_(chrono::high_resolution_clock::now())
    {
    }

    ~Stopwatch()
    {
        auto end_time = chrono::high_resolution_clock::now();
        auto duration = chrono::duration_cast<chrono::milliseconds>(end_time - start_time_);
        cout << name_ << ": " << duration.count() << " milliseconds" << endl;
    }

private:
    string name_;
    chrono::system_clock::time_point start_time_;
};


#endif //STOPWATCH_H
