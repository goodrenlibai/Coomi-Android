import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'
import ManualRequestCard from '../ManualRequestCard.vue'
import type { ManualCard } from '@/stores/viewModel'

function makeCard(overrides: Partial<ManualCard> = {}): ManualCard {
  return {
    kind: 'manual',
    id: 'manual-1',
    seq: 1,
    prompt: '提示词内容：请分析这个项目。',
    status: 'awaiting',
    ...overrides,
  }
}

describe('ManualRequestCard 渲染测试', () => {
  beforeEach(() => {
    Object.defineProperty(navigator, 'clipboard', {
      value: { writeText: vi.fn().mockResolvedValue(undefined) },
      configurable: true,
    })
  })

  it('等待态：渲染标题、步骤提示、复制按钮与粘贴输入框', () => {
    const wrapper = mount(ManualRequestCard, { props: { card: makeCard() } })
    const text = wrapper.text()
    expect(text).toContain('人工模式')
    expect(text).toContain('等待粘贴回答')
    expect(text).toContain('复制提示词')
    expect(wrapper.find('textarea').exists()).toBe(true)
    expect(text).toContain('确认执行')
  })

  it('点击复制按钮：写入剪贴板并切换为「已复制」', async () => {
    const wrapper = mount(ManualRequestCard, { props: { card: makeCard() } })
    await wrapper.find('.copy-btn').trigger('click')
    await flushPromises()
    await wrapper.vm.$nextTick()
    expect(navigator.clipboard.writeText).toHaveBeenCalledWith('提示词内容：请分析这个项目。')
    expect(wrapper.text()).toContain('已复制')
  })

  it('输入回答后点击「确认执行」发出 submit 事件', async () => {
    const wrapper = mount(ManualRequestCard, { props: { card: makeCard() } })
    const payload = '[{"name":"read_file","arguments":{"path":"a.txt"}}]'
    await wrapper.find('textarea').setValue(payload)
    expect(wrapper.find('.msubmit').attributes('disabled')).toBeUndefined()
    await wrapper.find('.msubmit').trigger('click')
    const emitted = wrapper.emitted('submit')
    expect(emitted).toBeTruthy()
    expect(emitted![0]).toEqual(['manual-1', payload])
  })

  it('空/空白输入：确认按钮禁用', async () => {
    const wrapper = mount(ManualRequestCard, { props: { card: makeCard() } })
    await wrapper.find('textarea').setValue('   ')
    expect(wrapper.find('.msubmit').attributes('disabled')).toBeDefined()
  })

  it('已提交态：不显示输入框，展开后回显粘贴的回答', async () => {
    const wrapper = mount(ManualRequestCard, {
      props: { card: makeCard({ status: 'submitted', response: '好的，我先读文件' }) },
    })
    expect(wrapper.find('textarea').exists()).toBe(false)
    expect(wrapper.text()).toContain('已提交')
    expect(wrapper.text()).toContain('你粘贴的回答')
    // 默认折叠，点击标题后展开并回显回答全文。
    expect(wrapper.text()).not.toContain('好的，我先读文件')
    await wrapper.find('.mblock.response .mblock-head').trigger('click')
    expect(wrapper.text()).toContain('好的，我先读文件')
  })

  it('已取消态：显示取消文案且无输入框与复制按钮', () => {
    const wrapper = mount(ManualRequestCard, { props: { card: makeCard({ status: 'cancelled' }) } })
    expect(wrapper.text()).toContain('已取消')
    expect(wrapper.text()).toContain('本轮已取消')
    expect(wrapper.find('textarea').exists()).toBe(false)
    expect(wrapper.find('.copy-btn').exists()).toBe(false)
  })

  it('历史会话缺失提示词：显示占位且不显示复制按钮', () => {
    const wrapper = mount(ManualRequestCard, { props: { card: makeCard({ prompt: '' }) } })
    expect(wrapper.text()).toContain('提示词未保留')
    expect(wrapper.find('.copy-btn').exists()).toBe(false)
  })
})
