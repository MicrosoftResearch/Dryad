// Learn more about F# at http://fsharp.net
// See the 'F# Tutorial' project for more help.

open System
open System.Collections.Generic
open System.Linq
open System.IO
open Microsoft.Research.DryadLinq

let context = new DryadLinqContext(1)
let inputUri = @"partfile:///d:/DryadLinqTemp/PartFiles/foo.pt"
let input = context.FromStore<LineRecord>(inputUri)

type Pair = { word : string; count : int }

let Histogram (source : IQueryable<LineRecord>) (k : int) =
    let words = source.SelectMany(fun (x : LineRecord) -> x.Line.Split(' ') |> Seq.ofArray)
    let groups = words.GroupBy(fun x -> x)
    let counts = groups.Select(fun (x : IGrouping<string, string>) -> { word = x.Key; count = x.Count() })
    let ordered = counts.OrderByDescending(fun x -> x.count)
    ordered.Take(k)
  
let Histogram1 (source : IQueryable<LineRecord>) (k : int) =
    query {
        for ws in source do
        for w in ws.Line.Split(' ') do
        groupBy w into g
        select { word = g.Key; count = g.Count() } into winfo
        sortByDescending winfo.count
        take k
        }

let Histogram2 = 
    query {
        for ws in input do
        for w in ws.Line.Split(' ') do
        groupBy w into g
        select { word = g.Key; count = g.Count() } into winfo
        sortByDescending winfo.count
        take 10
        }

for x in Histogram1 input 10 do System.Console.WriteLine(x.word + " : " + x.count.ToString())

//let foo = Histogram input 10
//foo.ToStore("partfile:///d:/DryadLinqTemp/PartFiles/xxx.pt").SubmitAndWait()
