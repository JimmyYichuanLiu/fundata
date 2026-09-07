import { Component } from 'react'
export default class ErrorBoundary extends Component {
  state = { failed: false }
  static getDerivedStateFromError() { return { failed: true } }
  render() {
    if (this.state.failed) return <div className="page-state" role="alert"><h1>页面暂时无法显示</h1><p>数据展示发生异常。可以重新载入当前页面，或返回基金概览继续浏览。</p><div className="flex justify-center gap-3 mt-5"><button className="button-primary" onClick={() => window.location.reload()}>重新载入</button><a className="button-secondary" href="/">基金概览</a></div></div>
    return this.props.children
  }
}
