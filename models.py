# noinspection PyPep8Naming
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Tuple


@dataclass(frozen=True, slots=True)
class PatientRecord:
    """Запись пациента для сверки."""
    fam: str
    im: str
    ot: str
    dr: str
    enp: str
    bp: str
    ep: str
    status: str
    comment: str
    okato: str

    @property
    def full_key(self) -> Tuple[str, ...]:
        return self.fam, self.im, self.ot, self.dr, self.enp, self.bp

    @property
    def get_errors(self) -> Tuple[str, ...]:
        return self.fam, self.im, self.ot, self.dr, self.enp, self.bp, self.status, self.comment

    @property
    def get_detached(self) -> Tuple[str, ...]:
        return self.fam, self.im, self.ot, self.dr, self.enp, self.bp, self.ep, self.comment

    @property
    def short_key(self) -> Tuple[str, str]:
        return self.enp, self.bp

    @classmethod
    def from_xml(cls, zap: ET.Element) -> 'PatientRecord':
        return cls(
            fam=zap.findtext('FAM', ''),
            im=zap.findtext('IM', ''),
            ot=zap.findtext('OT', ''),
            dr=zap.findtext('DR', ''),
            enp=zap.findtext('ENP', ''),
            bp=zap.findtext('BP', ''),
            ep=zap.findtext('EP', ''),
            status=zap.findtext('STATUS', ''),
            comment=zap.findtext('COMMENT', ''),
            okato=zap.findtext('OKATO', '')
        )

    @property
    def is_valid(self) -> bool:
        return bool(self.enp and self.bp)
