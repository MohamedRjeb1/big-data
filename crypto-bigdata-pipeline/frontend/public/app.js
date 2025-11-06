// Minimal React app (no build) — uses CDN React and axios
(function(){
  const e = React.createElement;
  const useState = React.useState;
  const useEffect = React.useEffect;

  function RealTimeList(){
    const [rows, setRows] = useState([]);
    useEffect(()=>{
      let mounted = true;
      async function fetcher(){
        try{
          const res = await axios.get((window.API_URL||'/api') + '/realtime');
          if(mounted) setRows(res.data || []);
        }catch(err){console.error(err)}
      }
      fetcher();
      const id = setInterval(fetcher, 3000);
      return ()=>{ mounted=false; clearInterval(id)};
    },[]);
    return e('div', null,
      e('h3', null, 'Realtime (last items)'),
      e('table', {border:1, cellPadding:6},
        e('thead', null, e('tr', null, ['symbol','window_start','avg_price','sum_volume','count_trades'].map(h=>e('th',null,h)))),
        e('tbody', null, rows.map((r,i)=>e('tr',{key:i},[
          e('td',null,r.symbol), e('td',null,r.window_start), e('td',null,r.avg_price), e('td',null,r.sum_volume), e('td',null,r.count_trades)
        ])))
      )
    );
  }

  function DailyList(){
    const [rows, setRows] = useState([]);
    useEffect(()=>{
      let mounted=true;
      async function fetcher(){
        try{const res = await axios.get((window.API_URL||'/api') + '/daily'); if(mounted) setRows(res.data||[])}catch(e){console.error(e)}
      }
      fetcher();
    },[]);
    return e('div', null,
      e('h3', null, 'Daily Aggregates'),
      e('table',{border:1,cellPadding:6},
        e('thead',null,e('tr',null,['symbol','date','avg_price','total_volume','max_price','min_price','count'].map(h=>e('th',null,h)))),
        e('tbody',null, rows.map((r,i)=>e('tr',{key:i},[
          e('td',null,r.symbol), e('td',null,r.date), e('td',null,r.avg_price), e('td',null,r.total_volume), e('td',null,r.max_price), e('td',null,r.min_price), e('td',null,r.count)
        ])))
      )
    )
  }

  function App(){
    return e('div', null, e('h1', null, 'Crypto BigData Pipeline Dashboard'), e(RealTimeList), e(DailyList));
  }

  ReactDOM.createRoot(document.getElementById('root')).render(e(App));
})();
