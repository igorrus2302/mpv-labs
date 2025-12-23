using System;
using System.Diagnostics;
using System.Threading.Tasks;

class Program
{
    const int MaxNumber = 100_000;

    static void Main()
    {
        Console.WriteLine("Параллельная версия");
        RunParallelVersion();

        Console.WriteLine();
        Console.WriteLine("Однопоточная версия");
        RunSingleThreadVersion();
    }

    static void RunParallelVersion()
    {
        long oddSum = 0;
        long evenSum = 0;

        Stopwatch sw = Stopwatch.StartNew();

        Task oddTask = Task.Run(() =>
        {
            for (int i = 1; i <= MaxNumber; i += 2)
            {
                oddSum += CalculateSumIterative(i);
            }
        });

        Task evenTask = Task.Run(() =>
        {
            for (int i = 2; i <= MaxNumber; i += 2)
            {
                evenSum += CalculateSumIterative(i);
            }
        });

        Task.WaitAll(oddTask, evenTask);

        sw.Stop();

        Console.WriteLine($"Сумма для нечётных чисел: {oddSum}");
        Console.WriteLine($"Сумма для чётных чисел:  {evenSum}");
        Console.WriteLine($"Общая сумма:            {oddSum + evenSum}");
        Console.WriteLine($"Время выполнения:       {sw.Elapsed.TotalSeconds:F3} сек");
    }

    static void RunSingleThreadVersion()
    {
        long totalSum = 0;

        Stopwatch sw = Stopwatch.StartNew();

        for (int i = 1; i <= MaxNumber; i++)
        {
            totalSum += CalculateSumIterative(i);
        }

        sw.Stop();

        Console.WriteLine($"Общая сумма:      {totalSum}");
        Console.WriteLine($"Время выполнения: {sw.Elapsed.TotalSeconds:F3} сек");
    }

    static long CalculateSumIterative(int n)
    {
        long sum = 0;
        for (int i = 0; i <= n; i++)
        {
            sum += i;
        }
        return sum;
    }
}
