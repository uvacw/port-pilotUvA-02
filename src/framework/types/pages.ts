import { isInstanceOf } from '../helpers'
import { PropsUIFooter, PropsUIHeader } from './elements'
import { PropsUIPromptFileInput, PropsUIPromptConfirm, PropsUIPromptConsentForm } from './prompts'

export type PropsUIPage =
  PropsUIPageSplashScreen |
  PropsUIPageDonation |
  PropsUIPageEnd

export function isPropsUIPage (arg: any): arg is PropsUIPage {
  return (
    isPropsUIPageSplashScreen(arg) ||
    isPropsUIPageDonation(arg) ||
    isPropsUIPageEnd(arg)
  )
}

export interface PropsUIPageSplashScreen {
  __type__: 'PropsUIPageSplashScreen'
}
export function isPropsUIPageSplashScreen (arg: any): arg is PropsUIPageSplashScreen {
  return isInstanceOf<PropsUIPageSplashScreen>(arg, 'PropsUIPageSplashScreen', [])
}

export interface PropsUIPageDonation {
  __type__: 'PropsUIPageDonation'
  platform: string
  header: PropsUIHeader
  body: PropsUIPromptFileInput | PropsUIPromptConfirm | PropsUIPromptConsentForm
  footer: PropsUIFooter
}
export function isPropsUIPageDonation (arg: any): arg is PropsUIPageDonation {
  return isInstanceOf<PropsUIPageDonation>(arg, 'PropsUIPageDonation', ['platform', 'header', 'body', 'footer'])
}

export interface PropsUIPageEnd {
  __type__: 'PropsUIPageEnd'
}
export function isPropsUIPageEnd (arg: any): arg is PropsUIPageEnd {
  return isInstanceOf<PropsUIPageEnd>(arg, 'PropsUIPageEnd', [])
}
